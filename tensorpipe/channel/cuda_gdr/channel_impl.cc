/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr/channel_impl.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <tensorpipe/channel/cuda_gdr/context_impl.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

ChannelImpl::ChannelImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::shared_ptr<transport::Connection> connection)
    : ChannelImplBoilerplate<CudaBuffer, ContextImpl, ChannelImpl>(
          token,
          std::move(context),
          std::move(id)),
      connection_(std::move(connection)) {}

void ChannelImpl::initImplFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, INITIALIZING);
  TP_DCHECK(!error_);

  context_->enroll(*this);

  localGpuToNic_ = context_->getGpuToNicMapping();
  numLocalNics_ =
      *std::max_element(localGpuToNic_.begin(), localGpuToNic_.end()) + 1;

  auto nopHolderOut = std::make_shared<NopHolder<HandshakeNumNics>>();
  HandshakeNumNics& nopHandshakeNumNics = nopHolderOut->getObject();
  nopHandshakeNumNics.numNics = numLocalNics_;
  TP_VLOG(6) << "Channel " << id_
             << " is writing nop object (handshake num NICs)";
  connection_->write(
      *nopHolderOut, lazyCallbackWrapper_([nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake num NICs)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeNumNics>>();
  TP_VLOG(6) << "Channel " << id_
             << " is reading nop object (handshake num NICs)";
  connection_->read(
      *nopHolderIn, lazyCallbackWrapper_([nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake num NICs)";
        impl.onReadHandshakeNumNics(nopHolderIn->getObject());
      }));

  state_ = WAITING_FOR_HANDSHAKE_NUM_NICS;
}

void ChannelImpl::onReadHandshakeNumNics(
    const HandshakeNumNics& nopHandshakeNumNics) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, WAITING_FOR_HANDSHAKE_NUM_NICS);
  TP_DCHECK(!error_);

  numRemoteNics_ = nopHandshakeNumNics.numNics;

  std::vector<std::vector<NopIbvSetupInformation>> allSetupInfo;

  queuePairs_.resize(numLocalNics_);
  allSetupInfo.resize(numLocalNics_);
  for (size_t localNicIdx = 0; localNicIdx < numLocalNics_; localNicIdx++) {
    queuePairs_[localNicIdx].resize(numRemoteNics_);
    allSetupInfo[localNicIdx].resize(numRemoteNics_);
    IbvNic& localNic = context_->getIbvNic(localNicIdx);
    for (size_t remoteNicIdx = 0; remoteNicIdx < numRemoteNics_;
         remoteNicIdx++) {
      IbvLib::qp_init_attr initAttr;
      std::memset(&initAttr, 0, sizeof(initAttr));
      initAttr.qp_type = IbvLib::QPT_RC;
      initAttr.send_cq = localNic.getIbvCq().get();
      initAttr.recv_cq = localNic.getIbvCq().get();
      initAttr.cap.max_send_wr = kNumSends;
      initAttr.cap.max_send_sge = 1;
      initAttr.cap.max_recv_wr = kNumRecvs;
      initAttr.cap.max_recv_sge = 1;
      initAttr.sq_sig_all = 1;
      IbvQueuePair qp = createIbvQueuePair(
          context_->getIbvLib(), localNic.getIbvPd(), initAttr);

      transitionIbvQueuePairToInit(
          context_->getIbvLib(), qp, localNic.getIbvAddress());

      IbvSetupInformation setupInfo =
          makeIbvSetupInformation(localNic.getIbvAddress(), qp);

      queuePairs_[localNicIdx][remoteNicIdx] = std::move(qp);
      allSetupInfo[localNicIdx][remoteNicIdx].fromIbvSetupInformation(
          setupInfo);
    }
  }

  auto nopHolderOut = std::make_shared<NopHolder<HandshakeSetupInfo>>();
  HandshakeSetupInfo& nopHandshakeSetupInfo = nopHolderOut->getObject();
  nopHandshakeSetupInfo.setupInfo = std::move(allSetupInfo);
  TP_VLOG(6) << "Channel " << id_ << " is writing nop object (handshake two)";
  connection_->write(
      *nopHolderOut, lazyCallbackWrapper_([nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake two)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeSetupInfo>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (handshake two)";
  connection_->read(
      *nopHolderIn, lazyCallbackWrapper_([nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake two)";
        impl.onReadHandshakeSetupInfo(nopHolderIn->getObject());
      }));

  state_ = WAITING_FOR_HANDSHAKE_SETUP_INFO;
}

void ChannelImpl::onReadHandshakeSetupInfo(
    const HandshakeSetupInfo& nopHandshakeSetupInfo) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, WAITING_FOR_HANDSHAKE_SETUP_INFO);
  TP_DCHECK(!error_);

  const std::vector<std::vector<NopIbvSetupInformation>>& remoteSetupInfo =
      nopHandshakeSetupInfo.setupInfo;

  TP_DCHECK_EQ(remoteSetupInfo.size(), numRemoteNics_);
  for (size_t remoteNicIdx = 0; remoteNicIdx < numRemoteNics_; remoteNicIdx++) {
    TP_DCHECK_EQ(remoteSetupInfo[remoteNicIdx].size(), numLocalNics_);
    for (size_t localNicIdx = 0; localNicIdx < numLocalNics_; localNicIdx++) {
      IbvNic& localNic = context_->getIbvNic(localNicIdx);
      IbvSetupInformation setupInfo =
          remoteSetupInfo[remoteNicIdx][localNicIdx].toIbvSetupInformation();

      transitionIbvQueuePairToReadyToReceive(
          context_->getIbvLib(),
          queuePairs_[localNicIdx][remoteNicIdx],
          localNic.getIbvAddress(),
          setupInfo);
      transitionIbvQueuePairToReadyToSend(
          context_->getIbvLib(), queuePairs_[localNicIdx][remoteNicIdx]);
    }
  }

  state_ = ESTABLISHED;
  for (auto& sendOp : sendOps_) {
    processSendOperationFromLoop(sendOp);
  }
  for (auto& recvOp : recvOps_) {
    processRecvOperationFromLoop(recvOp);
  }
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  size_t localGpuIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  size_t localNicIdx = context_->getGpuToNicMapping()[localGpuIdx];

  sendOps_.emplace_back(
      sequenceNumber, buffer, std::move(callback), localGpuIdx, localNicIdx);
  SendOperation& op = sendOps_.back();
  op.event.record(op.buffer.stream);
  if (state_ == ESTABLISHED) {
    processSendOperationFromLoop(op);
  }

  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.originNicIdx = localNicIdx;
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

void ChannelImpl::processSendOperationFromLoop(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  auto nopHolderIn = std::make_shared<NopHolder<ReadyToReceive>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading ready-to-receive (#"
             << op.sequenceNumber << ")";
  connection_->read(
      *nopHolderIn,
      eagerCallbackWrapper_([&op, nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading ready-to-receive (# " << op.sequenceNumber
                   << ")";
        impl.onReadReadyToReceive(op, nopHolderIn->getObject());
      }));
}

void ChannelImpl::onReadReadyToReceive(
    SendOperation& op,
    const ReadyToReceive& readyToReceive) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (error_) {
    op.callback(error_);
    eraseOp(op);
    return;
  }

  op.remoteNicIdx = readyToReceive.destinationNicIdx;

  TP_VLOG(6) << "Channel " << id_ << " is waiting for CUDA event to send (#"
             << op.sequenceNumber << ")";
  // FIXME There is no guarantee that two CUDA events will complete in the order
  // in which we add them (if they are on different streams). This could mean
  // that a later tensor might overtake an earlier one and issue its ibverbs
  // send earlier, thus messing up the order and causing a mismatch with the
  // receiver. The proper fix for this is a state machine, like the pipe has.
  context_->waitForCudaEvent(
      op.event, eagerCallbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done waiting for CUDA event to send (# "
                   << op.sequenceNumber << ")";
        impl.onSendEventReady(op);
      }));
}

void ChannelImpl::onSendEventReady(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (error_) {
    op.callback(error_);
    eraseOp(op);
    return;
  }

  IbvNic& localNic = context_->getIbvNic(op.localNicIdx);
  IbvQueuePair& qp = queuePairs_[op.localNicIdx][op.remoteNicIdx];

  // This could be VEEERY slow the first time we encounter the buffer, but the
  // result will be cached and subsequent calls will be much faster.
  IbvMemoryRegion& mr = localNic.registerMemory(op.buffer);

  IbvLib::sge list;
  list.addr = reinterpret_cast<uint64_t>(op.buffer.ptr);
  list.length = op.buffer.length;
  list.lkey = mr->lkey;

  IbvLib::send_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  wr.sg_list = &list;
  wr.num_sge = 1;
  wr.opcode = IbvLib::WR_SEND;

  TP_VLOG(6) << "Channel " << id_ << " is sending tensor (#"
             << op.sequenceNumber << ") on QP " << qp->qp_num;
  localNic.postSend(qp, wr, eagerCallbackWrapper_([&op](ChannelImpl& impl) {
                      TP_VLOG(6) << "Channel " << impl.id_
                                 << " done sending tensor (# "
                                 << op.sequenceNumber << ")";
                      impl.onIbvSendDone(op);
                    }));
  numSendsInFlight_++;
}

void ChannelImpl::onIbvSendDone(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  numSendsInFlight_--;

  op.callback(error_);
  eraseOp(op);

  tryCleanup();
}

void ChannelImpl::eraseOp(const SendOperation& op) {
  auto iter = std::find_if(
      sendOps_.begin(), sendOps_.end(), [&](const SendOperation& otherOp) {
        return otherOp.sequenceNumber == op.sequenceNumber;
      });
  TP_DCHECK(iter != sendOps_.end());
  sendOps_.erase(iter);
}

void ChannelImpl::recvImplFromLoop(
    uint64_t sequenceNumber,
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  size_t localGpuIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  size_t localNicIdx = context_->getGpuToNicMapping()[localGpuIdx];

  NopHolder<Descriptor> nopHolder;
  loadDescriptor(nopHolder, descriptor);
  Descriptor& nopDescriptor = nopHolder.getObject();
  size_t remoteNicIdx = nopDescriptor.originNicIdx;

  recvOps_.emplace_back(
      sequenceNumber,
      buffer,
      std::move(callback),
      localGpuIdx,
      localNicIdx,
      remoteNicIdx);
  RecvOperation& op = recvOps_.back();
  op.event.record(op.buffer.stream);
  if (state_ == ESTABLISHED) {
    processRecvOperationFromLoop(op);
  }
}

void ChannelImpl::processRecvOperationFromLoop(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  TP_VLOG(6) << "Channel " << id_ << " is waiting for CUDA event to recv (#"
             << op.sequenceNumber << ")";
  // FIXME There is no guarantee that two CUDA events will complete in the order
  // in which we add them (if they are on different streams). This could mean
  // that a later tensor might overtake an earlier one and issue its ibverbs
  // recv earlier, thus messing up the order and causing a mismatch with the
  // sender. The proper fix for this is a state machine, like the pipe has.
  context_->waitForCudaEvent(
      op.event, eagerCallbackWrapper_([&op](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done waiting for CUDA event to recv (# "
                   << op.sequenceNumber << ")";
        impl.onRecvEventReady(op);
      }));
}

void ChannelImpl::onRecvEventReady(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  if (error_) {
    op.callback(error_);
    eraseOp(op);
    return;
  }

  IbvNic& localNic = context_->getIbvNic(op.localNicIdx);
  IbvQueuePair& qp = queuePairs_[op.localNicIdx][op.remoteNicIdx];

  // This could be VEEERY slow the first time we encounter the buffer, but the
  // result will be cached and subsequent calls will be much faster.
  IbvMemoryRegion& mr = localNic.registerMemory(op.buffer);

  IbvLib::sge list;
  list.addr = reinterpret_cast<uint64_t>(op.buffer.ptr);
  list.length = op.buffer.length;
  list.lkey = mr->lkey;

  IbvLib::recv_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  wr.sg_list = &list;
  wr.num_sge = 1;

  TP_VLOG(6) << "Channel " << id_ << " is receiving tensor (#"
             << op.sequenceNumber << ") on QP " << qp->qp_num;
  localNic.postRecv(qp, wr, eagerCallbackWrapper_([&op](ChannelImpl& impl) {
                      TP_VLOG(6) << "Channel " << impl.id_
                                 << " done receiving tensor (# "
                                 << op.sequenceNumber << ")";
                      impl.onIbvRecvDone(op);
                    }));
  numRecvsInFlight_++;

  auto nopHolderOut = std::make_shared<NopHolder<ReadyToReceive>>();
  ReadyToReceive& nopReadyToReceive = nopHolderOut->getObject();
  nopReadyToReceive.destinationNicIdx = op.localNicIdx;
  TP_VLOG(6) << "Channel " << id_ << " is writing ready-to-receive (#"
             << op.sequenceNumber << ")";
  connection_->write(
      *nopHolderOut,
      lazyCallbackWrapper_(
          [sequenceNumber{op.sequenceNumber}, nopHolderOut](ChannelImpl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing ready-to-receive (#" << sequenceNumber
                       << ")";
          }));
}

void ChannelImpl::onIbvRecvDone(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  numRecvsInFlight_--;

  op.callback(error_);
  eraseOp(op);

  tryCleanup();
}

void ChannelImpl::eraseOp(const RecvOperation& op) {
  auto iter = std::find_if(
      recvOps_.begin(), recvOps_.end(), [&](const RecvOperation& otherOp) {
        return otherOp.sequenceNumber == op.sequenceNumber;
      });
  TP_DCHECK(iter != recvOps_.end());
  recvOps_.erase(iter);
}

void ChannelImpl::handleErrorImpl() {
  if (state_ != ESTABLISHED) {
    // No operation has yet started being served, hence they can all be safely
    // aborted.
    for (auto& sendOp : sendOps_) {
      sendOp.callback(error_);
    }
    sendOps_.clear();
    for (auto& recvOp : recvOps_) {
      recvOp.callback(error_);
    }
    recvOps_.clear();
  } else {
    // All operations are currently waiting for some lower-level operation to
    // return. We will take care of calling the callback and easing each of them
    // once their current operation terminates.
  }

  for (size_t localNicIdx = 0; localNicIdx < numLocalNics_; localNicIdx++) {
    for (size_t remoteNicIdx = 0; remoteNicIdx < numRemoteNics_;
         remoteNicIdx++) {
      transitionIbvQueuePairToError(
          context_->getIbvLib(), queuePairs_[localNicIdx][remoteNicIdx]);
    }
  }

  tryCleanup();

  connection_->close();
}

void ChannelImpl::tryCleanup() {
  TP_DCHECK(context_->inLoop());

  if (error_) {
    if (numSendsInFlight_ == 0 && numRecvsInFlight_ == 0) {
      cleanup();
    } else {
      TP_VLOG(9) << "Connection " << id_
                 << " cannot proceed to cleanup because it has "
                 << numSendsInFlight_ << " pending send requests and "
                 << numRecvsInFlight_ << " pending recv requests";
    }
  }
}

void ChannelImpl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Connection " << id_ << " is cleaning up";

  queuePairs_.clear();

  context_->unenroll(*this);
}

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
