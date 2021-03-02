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
      *nopHolderOut, callbackWrapper_([nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake num NICs)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeNumNics>>();
  TP_VLOG(6) << "Channel " << id_
             << " is reading nop object (handshake num NICs)";
  connection_->read(
      *nopHolderIn, callbackWrapper_([nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake num NICs)";
        if (!impl.error_) {
          impl.onReadHandshakeNumNics(nopHolderIn->getObject());
        }
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
      *nopHolderOut, callbackWrapper_([nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake two)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeSetupInfo>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (handshake two)";
  connection_->read(
      *nopHolderIn, callbackWrapper_([nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake two)";
        if (!impl.error_) {
          impl.onReadHandshakeSetupInfo(nopHolderIn->getObject());
        }
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
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();
}

void ChannelImpl::sendImplFromLoop(
    uint64_t sequenceNumber,
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  size_t localGpuIdx = cudaDeviceForPointer(context_->getCudaLib(), buffer.ptr);
  size_t localNicIdx = context_->getGpuToNicMapping()[localGpuIdx];

  SendOpIter opIter = sendOps_.emplaceBack(
      sequenceNumber, buffer, std::move(callback), localGpuIdx, localNicIdx);
  opIter->event.record(buffer.stream);

  sendOps_.advanceOperation(opIter);

  NopHolder<Descriptor> nopHolder;
  Descriptor& nopDescriptor = nopHolder.getObject();
  nopDescriptor.originNicIdx = localNicIdx;
  descriptorCallback(Error::kSuccess, saveDescriptor(nopHolder));
}

void ChannelImpl::advanceSendOperation(
    SendOpIter opIter,
    SendOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake.

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_,
      /*action=*/&ChannelImpl::callSendCallback);

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of read calls on control connection.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::UNINITIALIZED,
      /*to=*/SendOperation::READING_READY_TO_RECEIVE,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >= SendOperation::READING_READY_TO_RECEIVE,
      /*action=*/&ChannelImpl::writeReadyToSendAndReadReadyToReceive);

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_READY_TO_RECEIVE,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ && opIter->readReadyToReceive,
      /*action=*/&ChannelImpl::callSendCallback);

  // This doesn't strictly need to go after the previous op, but it doesn't make
  // sense to busy poll multiple events if only one of them is actually able to
  // then make progress.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::READING_READY_TO_RECEIVE,
      /*to=*/SendOperation::WAITING_FOR_CUDA_EVENT,
      /*cond=*/!error_ && opIter->readReadyToReceive &&
          prevOpState >= SendOperation::SENDING_OVER_IB,
      /*action=*/&ChannelImpl::waitForSendCudaEvent);

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WAITING_FOR_CUDA_EVENT,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/error_ && opIter->sendEventReady,
      /*action=*/&ChannelImpl::callSendCallback);

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of send calls on InfiniBand queue pair.
  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::WAITING_FOR_CUDA_EVENT,
      /*to=*/SendOperation::SENDING_OVER_IB,
      /*cond=*/!error_ && opIter->sendEventReady &&
          prevOpState >= SendOperation::SENDING_OVER_IB,
      /*action=*/&ChannelImpl::sendOverIb);

  sendOps_.attemptTransition(
      opIter,
      /*from=*/SendOperation::SENDING_OVER_IB,
      /*to=*/SendOperation::FINISHED,
      /*cond=*/opIter->sentOverIb,
      /*action=*/&ChannelImpl::callSendCallback);
}

void ChannelImpl::writeReadyToSendAndReadReadyToReceive(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::UNINITIALIZED);
  op.state = SendOperation::READING_READY_TO_RECEIVE;

  auto nopHolderIn = std::make_shared<NopHolder<ReadyToReceive>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading ready-to-receive (#"
             << op.sequenceNumber << ")";
  connection_->read(
      *nopHolderIn, callbackWrapper_([opIter, nopHolderIn](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading ready-to-receive (# "
                   << opIter->sequenceNumber << ")";
        impl.onReadReadyToReceive(opIter, nopHolderIn->getObject());
      }));
}

void ChannelImpl::onReadReadyToReceive(
    SendOpIter opIter,
    const ReadyToReceive& readyToReceive) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::READING_READY_TO_RECEIVE);

  op.readReadyToReceive = true;
  op.remoteNicIdx = readyToReceive.destinationNicIdx;

  sendOps_.advanceOperation(opIter);
}

void ChannelImpl::waitForSendCudaEvent(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::READING_READY_TO_RECEIVE);
  op.state = SendOperation::WAITING_FOR_CUDA_EVENT;

  TP_VLOG(6) << "Channel " << id_ << " is waiting for CUDA event to send (#"
             << op.sequenceNumber << ")";
  context_->waitForCudaEvent(
      op.event, callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done waiting for CUDA event to send (# "
                   << opIter->sequenceNumber << ")";
        impl.onSendEventReady(opIter);
      }));
}

void ChannelImpl::onSendEventReady(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::WAITING_FOR_CUDA_EVENT);

  op.sendEventReady = true;

  sendOps_.advanceOperation(opIter);
}

void ChannelImpl::sendOverIb(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::WAITING_FOR_CUDA_EVENT);
  op.state = SendOperation::SENDING_OVER_IB;

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
  localNic.postSend(qp, wr, callbackWrapper_([opIter](ChannelImpl& impl) {
                      TP_VLOG(6) << "Channel " << impl.id_
                                 << " done sending tensor (# "
                                 << opIter->sequenceNumber << ")";
                      impl.onIbvSendDone(opIter);
                    }));
  numSendsInFlight_++;
}

void ChannelImpl::onIbvSendDone(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  SendOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, SendOperation::SENDING_OVER_IB);

  op.sentOverIb = true;

  sendOps_.advanceOperation(opIter);

  numSendsInFlight_--;
  tryCleanup();
}

void ChannelImpl::callSendCallback(SendOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake.
  // Similarly, don't check for !error_.

  SendOperation& op = *opIter;
  // Don't check op.state: it can be called for many previous states.
  op.state = SendOperation::FINISHED;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
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

  RecvOpIter opIter = recvOps_.emplaceBack(
      sequenceNumber,
      buffer,
      std::move(callback),
      localGpuIdx,
      localNicIdx,
      remoteNicIdx);
  opIter->event.record(buffer.stream);

  recvOps_.advanceOperation(opIter);
}

void ChannelImpl::advanceRecvOperation(
    RecvOpIter opIter,
    RecvOperation::State prevOpState) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake.

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_,
      /*action=*/&ChannelImpl::callRecvCallback);

  // This doesn't strictly need to go after the previous op, but it doesn't make
  // sense to busy poll multiple events if only one of them is actually able to
  // then make progress.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::UNINITIALIZED,
      /*to=*/RecvOperation::WAITING_FOR_CUDA_EVENT,
      /*cond=*/!error_ && state_ == ESTABLISHED &&
          prevOpState >=
              RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE,
      /*action=*/&ChannelImpl::waitForRecvCudaEvent);

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::WAITING_FOR_CUDA_EVENT,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/error_ && opIter->recvEventReady,
      /*action=*/&ChannelImpl::callRecvCallback);

  // Needs to go after previous op to ensure predictable and consistent ordering
  // of recv calls on InfiniBand queue pair and write calls on control
  // connection.
  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::WAITING_FOR_CUDA_EVENT,
      /*to=*/RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE,
      /*cond=*/!error_ && opIter->recvEventReady &&
          prevOpState >=
              RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE,
      /*action=*/&ChannelImpl::recvOverIbAndWriteReadyToRecive);

  recvOps_.attemptTransition(
      opIter,
      /*from=*/RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE,
      /*to=*/RecvOperation::FINISHED,
      /*cond=*/opIter->receivedOverIb,
      /*action=*/&ChannelImpl::callRecvCallback);
}

void ChannelImpl::waitForRecvCudaEvent(RecvOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  RecvOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, RecvOperation::UNINITIALIZED);
  op.state = RecvOperation::WAITING_FOR_CUDA_EVENT;

  TP_VLOG(6) << "Channel " << id_ << " is waiting for CUDA event to recv (#"
             << op.sequenceNumber << ")";
  context_->waitForCudaEvent(
      op.event, callbackWrapper_([opIter](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done waiting for CUDA event to recv (# "
                   << opIter->sequenceNumber << ")";
        impl.onRecvEventReady(opIter);
      }));
}

void ChannelImpl::onRecvEventReady(RecvOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  RecvOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, RecvOperation::WAITING_FOR_CUDA_EVENT);

  op.recvEventReady = true;

  recvOps_.advanceOperation(opIter);
}

void ChannelImpl::recvOverIbAndWriteReadyToRecive(RecvOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  RecvOperation& op = *opIter;
  TP_DCHECK_EQ(op.state, RecvOperation::WAITING_FOR_CUDA_EVENT);
  op.state = RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE;

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
  localNic.postRecv(qp, wr, callbackWrapper_([opIter](ChannelImpl& impl) {
                      TP_VLOG(6) << "Channel " << impl.id_
                                 << " done receiving tensor (# "
                                 << opIter->sequenceNumber << ")";
                      impl.onReceivedOverIb(opIter);
                    }));
  numRecvsInFlight_++;

  auto nopHolderOut = std::make_shared<NopHolder<ReadyToReceive>>();
  ReadyToReceive& nopReadyToReceive = nopHolderOut->getObject();
  nopReadyToReceive.destinationNicIdx = op.localNicIdx;
  TP_VLOG(6) << "Channel " << id_ << " is writing ready-to-receive (#"
             << op.sequenceNumber << ")";
  connection_->write(
      *nopHolderOut,
      callbackWrapper_([sequenceNumber{opIter->sequenceNumber},
                        nopHolderOut](ChannelImpl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing ready-to-receive (#" << sequenceNumber
                   << ")";
      }));
}

void ChannelImpl::onReceivedOverIb(RecvOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  RecvOperation& op = *opIter;
  TP_DCHECK_EQ(
      op.state, RecvOperation::RECEIVING_OVER_IB_AND_WRITING_READY_TO_RECEIVE);

  op.receivedOverIb = true;

  recvOps_.advanceOperation(opIter);

  numRecvsInFlight_--;
  tryCleanup();
}

void ChannelImpl::callRecvCallback(RecvOpIter opIter) {
  TP_DCHECK(context_->inLoop());
  // Don't check state_ == ESTABLISHED: it can be called after failed handshake.
  // Similarly, don't check for !error_.

  RecvOperation& op = *opIter;
  // Don't check op.state: it can be called for many previous states.
  op.state = RecvOperation::FINISHED;

  op.callback(error_);
  // Reset callback to release the resources it was holding.
  op.callback = nullptr;
}

void ChannelImpl::handleErrorImpl() {
  sendOps_.advanceAllOperations();
  recvOps_.advanceAllOperations();

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
