/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cuda_gdr/channel.h>

#include <algorithm>
#include <list>

#include <nop/serializer.h>
#include <nop/structure.h>

#include <tensorpipe/channel/cuda_gdr/context_impl.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/ibv.h>
#include <tensorpipe/transport/connection.h>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

namespace {

// Replicate the IbvLib::gid struct so we can serialize it with libnop.
struct NopIbvGid {
  uint64_t subnetPrefix;
  uint64_t interfaceId;
  NOP_STRUCTURE(NopIbvGid, subnetPrefix, interfaceId);

  void fromIbvGid(const IbvLib::gid& globalIdentifier) {
    subnetPrefix = globalIdentifier.global.subnet_prefix;
    interfaceId = globalIdentifier.global.interface_id;
  }

  IbvLib::gid toIbvGid() const {
    IbvLib::gid globalIdentifier;
    globalIdentifier.global.subnet_prefix = subnetPrefix;
    globalIdentifier.global.interface_id = interfaceId;
    return globalIdentifier;
  }
};

// Replicate the IbvSetupInformation struct so we can serialize it with libnop.
struct NopIbvSetupInformation {
  // This pointless constructor is needed to work around a bug in GCC 5.5 (and
  // possibly other versions). It appears to be needed in the nop types that
  // are used inside std::vectors.
  NopIbvSetupInformation() {}

  uint32_t localIdentifier;
  NopIbvGid globalIdentifier;
  uint32_t queuePairNumber;
  IbvLib::mtu maximumTransmissionUnit;
  NOP_STRUCTURE(
      NopIbvSetupInformation,
      localIdentifier,
      globalIdentifier,
      queuePairNumber,
      maximumTransmissionUnit);

  void fromIbvSetupInformation(const IbvSetupInformation& setupInfo) {
    localIdentifier = setupInfo.localIdentifier;
    globalIdentifier.fromIbvGid(setupInfo.globalIdentifier);
    queuePairNumber = setupInfo.queuePairNumber;
    maximumTransmissionUnit = setupInfo.maximumTransmissionUnit;
  }

  IbvSetupInformation toIbvSetupInformation() const {
    IbvSetupInformation setupInfo;
    setupInfo.localIdentifier = localIdentifier;
    setupInfo.globalIdentifier = globalIdentifier.toIbvGid();
    setupInfo.queuePairNumber = queuePairNumber;
    setupInfo.maximumTransmissionUnit = maximumTransmissionUnit;
    return setupInfo;
  }
};

struct SendOperation {
  // Provide a constructor so we can create the CudaEvent in-place.
  SendOperation(
      size_t sequenceNumber,
      CudaBuffer buffer,
      TRecvCallback callback,
      size_t localGpuIdx,
      size_t localNicIdx)
      : sequenceNumber(sequenceNumber),
        buffer(buffer),
        callback(std::move(callback)),
        event(localGpuIdx),
        localNicIdx(localNicIdx) {}

  size_t sequenceNumber;
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  size_t remoteNicIdx;
};

struct RecvOperation {
  // Provide a constructor so we can create the CudaEvent in-place.
  RecvOperation(
      size_t sequenceNumber,
      CudaBuffer buffer,
      TSendCallback callback,
      size_t deviceIdx,
      size_t localNicIdx,
      size_t remoteNicIdx)
      : sequenceNumber(sequenceNumber),
        buffer(buffer),
        callback(std::move(callback)),
        event(deviceIdx),
        localNicIdx(localNicIdx),
        remoteNicIdx(remoteNicIdx) {}

  size_t sequenceNumber;
  CudaBuffer buffer;
  TSendCallback callback;
  CudaEvent event;
  size_t localNicIdx;
  size_t remoteNicIdx;
};

// First "round" of handshake.
struct HandshakeNumNics {
  size_t numNics;
  NOP_STRUCTURE(HandshakeNumNics, numNics);
};

// Second "round" of handshake.
struct HandshakeSetupInfo {
  std::vector<std::vector<NopIbvSetupInformation>> setupInfo;
  NOP_STRUCTURE(HandshakeSetupInfo, setupInfo);
};

// From sender to receiver (through pipe).
struct Descriptor {
  size_t originNicIdx;
  NOP_STRUCTURE(Descriptor, originNicIdx);
};

// From receiver to sender (through channel's connection).
struct ReadyToReceive {
  size_t destinationNicIdx;
  NOP_STRUCTURE(ReadyToReceive, destinationNicIdx);
};

} // namespace

class Channel::Impl : public std::enable_shared_from_this<Channel::Impl> {
 public:
  Impl(
      std::shared_ptr<Context::Impl> context,
      std::shared_ptr<transport::Connection> connection,
      std::string id);

  // Called by the channel's constructor.
  void init();

  void send(
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  void recv(TDescriptor descriptor, CudaBuffer buffer, TRecvCallback callback);

  // Tell the channel what its identifier is.
  void setId(std::string id);

  void close();

 private:
  void initFromLoop();

  // Send memory region to peer.
  void sendFromLoop(
      CudaBuffer buffer,
      TDescriptorCallback descriptorCallback,
      TSendCallback callback);

  // Receive memory region from peer.
  void recvFromLoop(
      TDescriptor descriptor,
      CudaBuffer buffer,
      TRecvCallback callback);

  void closeFromLoop();

  void setError(Error error);

  void setIdFromLoop(std::string id);

  // Helper function to process transport error.
  // Shared between read and write callback entry points.
  void handleError();

  std::shared_ptr<Context::Impl> context_;
  std::shared_ptr<transport::Connection> connection_;
  Error error_{Error::kSuccess};

  ClosingReceiver closingReceiver_;

  // Increasing identifier for send operations.
  uint64_t nextTensorBeingSent_{0};

  // Increasing identifier for recv operations.
  uint64_t nextTensorBeingReceived_{0};

  // An identifier for the channel, composed of the identifier for the context,
  // combined with an increasing sequence number. It will only be used for
  // logging and debugging purposes.
  std::string id_;

  enum State {
    INITIALIZING = 1,
    WAITING_FOR_HANDSHAKE_NUM_NICS,
    WAITING_FOR_HANDSHAKE_SETUP_INFO,
    ESTABLISHED,
  };
  State state_{INITIALIZING};

  void onReadHandshakeNumNics(const HandshakeNumNics& nopHandshakeNumNics);
  void onReadHandshakeSetupInfo(
      const HandshakeSetupInfo& nopHandshakeSetupInfo);

  std::vector<size_t> localGpuToNic_;
  size_t numLocalNics_;
  size_t numRemoteNics_;

  std::vector<std::vector<IbvQueuePair>> queuePairs_;

  std::list<SendOperation> sendOps_;
  std::list<RecvOperation> recvOps_;

  uint32_t numSendsInFlight_{0};
  uint32_t numRecvsInFlight_{0};

  void processSendOperationFromLoop(SendOperation& op);
  void onReadReadyToReceive(
      SendOperation& op,
      const ReadyToReceive& readyToReceive);
  void onSendEventReady(SendOperation& op);
  void onIbvSendDone(SendOperation& op);
  void eraseOp(const SendOperation& op);

  void processRecvOperationFromLoop(RecvOperation& op);
  void onRecvEventReady(RecvOperation& op);
  void onIbvRecvDone(RecvOperation& op);
  void eraseOp(const RecvOperation& op);

  void tryCleanup();
  void cleanup();

  LazyCallbackWrapper<Impl> lazyCallbackWrapper_{*this, *this->context_};
  EagerCallbackWrapper<Impl> eagerCallbackWrapper_{*this, *this->context_};

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::LazyCallbackWrapper;
  template <typename T>
  friend class tensorpipe::EagerCallbackWrapper;
};

Channel::Channel(
    ConstructorToken /* unused */,
    std::shared_ptr<Context::Impl> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : impl_(std::make_shared<Impl>(
          std::move(context),
          std::move(connection),
          std::move(id))) {
  impl_->init();
}

Channel::Impl::Impl(
    std::shared_ptr<Context::Impl> context,
    std::shared_ptr<transport::Connection> connection,
    std::string id)
    : context_(std::move(context)),
      connection_(std::move(connection)),
      closingReceiver_(context_, context_->getClosingEmitter()),
      id_(std::move(id)) {}

void Channel::Impl::init() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->initFromLoop(); });
}

void Channel::Impl::initFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, INITIALIZING);
  TP_DCHECK(!error_);

  closingReceiver_.activate(*this);

  localGpuToNic_ = context_->getGpuToNicMapping();
  numLocalNics_ =
      *std::max_element(localGpuToNic_.begin(), localGpuToNic_.end()) + 1;

  auto nopHolderOut = std::make_shared<NopHolder<HandshakeNumNics>>();
  HandshakeNumNics& nopHandshakeNumNics = nopHolderOut->getObject();
  nopHandshakeNumNics.numNics = numLocalNics_;
  TP_VLOG(6) << "Channel " << id_
             << " is writing nop object (handshake num NICs)";
  connection_->write(
      *nopHolderOut, lazyCallbackWrapper_([nopHolderOut](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake num NICs)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeNumNics>>();
  TP_VLOG(6) << "Channel " << id_
             << " is reading nop object (handshake num NICs)";
  connection_->read(
      *nopHolderIn, lazyCallbackWrapper_([nopHolderIn](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake num NICs)";
        impl.onReadHandshakeNumNics(nopHolderIn->getObject());
      }));

  state_ = WAITING_FOR_HANDSHAKE_NUM_NICS;
}

void Channel::Impl::onReadHandshakeNumNics(
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
      *nopHolderOut, lazyCallbackWrapper_([nopHolderOut](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done writing nop object (handshake two)";
      }));

  auto nopHolderIn = std::make_shared<NopHolder<HandshakeSetupInfo>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading nop object (handshake two)";
  connection_->read(
      *nopHolderIn, lazyCallbackWrapper_([nopHolderIn](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading nop object (handshake two)";
        impl.onReadHandshakeSetupInfo(nopHolderIn->getObject());
      }));

  state_ = WAITING_FOR_HANDSHAKE_SETUP_INFO;
}

void Channel::Impl::onReadHandshakeSetupInfo(
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

void Channel::send(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  impl_->send(buffer, std::move(descriptorCallback), std::move(callback));
}

void Channel::Impl::send(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  context_->deferToLoop([impl{this->shared_from_this()},
                         buffer,
                         descriptorCallback{std::move(descriptorCallback)},
                         callback{std::move(callback)}]() mutable {
    impl->sendFromLoop(
        buffer, std::move(descriptorCallback), std::move(callback));
  });
}

void Channel::Impl::sendFromLoop(
    CudaBuffer buffer,
    TDescriptorCallback descriptorCallback,
    TSendCallback callback) {
  TP_DCHECK(context_->inLoop());

  const uint64_t sequenceNumber = nextTensorBeingSent_++;
  TP_VLOG(4) << "Channel " << id_ << " received a send request (#"
             << sequenceNumber << ")";

  descriptorCallback = [this,
                        sequenceNumber,
                        descriptorCallback{std::move(descriptorCallback)}](
                           const Error& error, TDescriptor descriptor) {
    // There is no requirement for the channel to invoke callbacks in order.
    TP_VLOG(4) << "Channel " << id_ << " is calling a descriptor callback (#"
               << sequenceNumber << ")";
    descriptorCallback(error, std::move(descriptor));
    TP_VLOG(4) << "Channel " << id_ << " done calling a descriptor callback (#"
               << sequenceNumber << ")";
  };

  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    TP_VLOG(4) << "Channel " << id_ << " is calling a send callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a send callback (#"
               << sequenceNumber << ")";
  };

  if (error_ || buffer.length == 0) {
    descriptorCallback(error_, std::string());
    callback(error_);
    return;
  }

  size_t localGpuIdx = cudaDeviceForPointer(buffer.ptr);
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

void Channel::Impl::processSendOperationFromLoop(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);
  TP_DCHECK(!error_);

  auto nopHolderIn = std::make_shared<NopHolder<ReadyToReceive>>();
  TP_VLOG(6) << "Channel " << id_ << " is reading ready-to-receive (#"
             << op.sequenceNumber << ")";
  connection_->read(
      *nopHolderIn, eagerCallbackWrapper_([&op, nopHolderIn](Impl& impl) {
        TP_VLOG(6) << "Channel " << impl.id_
                   << " done reading ready-to-receive (# " << op.sequenceNumber
                   << ")";
        impl.onReadReadyToReceive(op, nopHolderIn->getObject());
      }));
}

void Channel::Impl::onReadReadyToReceive(
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
  context_->waitForCudaEvent(op.event, eagerCallbackWrapper_([&op](Impl& impl) {
                               TP_VLOG(6)
                                   << "Channel " << impl.id_
                                   << " done waiting for CUDA event to send (# "
                                   << op.sequenceNumber << ")";
                               impl.onSendEventReady(op);
                             }));
}

void Channel::Impl::onSendEventReady(SendOperation& op) {
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
  localNic.postSend(qp, wr, eagerCallbackWrapper_([&op](Impl& impl) {
                      TP_VLOG(6) << "Channel " << impl.id_
                                 << " done sending tensor (# "
                                 << op.sequenceNumber << ")";
                      impl.onIbvSendDone(op);
                    }));
  numSendsInFlight_++;
}

void Channel::Impl::onIbvSendDone(SendOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  numSendsInFlight_--;

  op.callback(error_);
  eraseOp(op);

  tryCleanup();
}

void Channel::Impl::eraseOp(const SendOperation& op) {
  auto iter = std::find_if(
      sendOps_.begin(), sendOps_.end(), [&](const SendOperation& otherOp) {
        return otherOp.sequenceNumber == op.sequenceNumber;
      });
  TP_DCHECK(iter != sendOps_.end());
  sendOps_.erase(iter);
}

// Receive memory region from peer.
void Channel::recv(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  impl_->recv(std::move(descriptor), buffer, std::move(callback));
}

void Channel::Impl::recv(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  context_->deferToLoop([impl{this->shared_from_this()},
                         descriptor{std::move(descriptor)},
                         buffer,
                         callback{std::move(callback)}]() mutable {
    impl->recvFromLoop(std::move(descriptor), buffer, std::move(callback));
  });
}

void Channel::Impl::recvFromLoop(
    TDescriptor descriptor,
    CudaBuffer buffer,
    TRecvCallback callback) {
  TP_DCHECK(context_->inLoop());

  const uint64_t sequenceNumber = nextTensorBeingReceived_++;
  TP_VLOG(4) << "Channel " << id_ << " received a recv request (#"
             << sequenceNumber << ")";

  callback = [this, sequenceNumber, callback{std::move(callback)}](
                 const Error& error) {
    TP_VLOG(4) << "Channel " << id_ << " is calling a recv callback (#"
               << sequenceNumber << ")";
    callback(error);
    TP_VLOG(4) << "Channel " << id_ << " done calling a recv callback (#"
               << sequenceNumber << ")";
  };

  if (error_ || buffer.length == 0) {
    callback(error_);
    return;
  }

  size_t localGpuIdx = cudaDeviceForPointer(buffer.ptr);
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

void Channel::Impl::processRecvOperationFromLoop(RecvOperation& op) {
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
  context_->waitForCudaEvent(op.event, eagerCallbackWrapper_([&op](Impl& impl) {
                               TP_VLOG(6)
                                   << "Channel " << impl.id_
                                   << " done waiting for CUDA event to recv (# "
                                   << op.sequenceNumber << ")";
                               impl.onRecvEventReady(op);
                             }));
}

void Channel::Impl::onRecvEventReady(RecvOperation& op) {
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
  localNic.postRecv(qp, wr, eagerCallbackWrapper_([&op](Impl& impl) {
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
          [sequenceNumber{op.sequenceNumber}, nopHolderOut](Impl& impl) {
            TP_VLOG(6) << "Channel " << impl.id_
                       << " done writing ready-to-receive (#" << sequenceNumber
                       << ")";
          }));
}

void Channel::Impl::onIbvRecvDone(RecvOperation& op) {
  TP_DCHECK(context_->inLoop());
  TP_DCHECK_EQ(state_, ESTABLISHED);

  numRecvsInFlight_--;

  op.callback(error_);
  eraseOp(op);

  tryCleanup();
}

void Channel::Impl::eraseOp(const RecvOperation& op) {
  auto iter = std::find_if(
      recvOps_.begin(), recvOps_.end(), [&](const RecvOperation& otherOp) {
        return otherOp.sequenceNumber == op.sequenceNumber;
      });
  TP_DCHECK(iter != recvOps_.end());
  recvOps_.erase(iter);
}

void Channel::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Channel::Impl::setId(std::string id) {
  context_->deferToLoop(
      [impl{this->shared_from_this()}, id{std::move(id)}]() mutable {
        impl->setIdFromLoop(std::move(id));
      });
}

void Channel::Impl::setIdFromLoop(std::string id) {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(4) << "Channel " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

void Channel::close() {
  impl_->close();
}

Channel::~Channel() {
  close();
}

void Channel::Impl::close() {
  context_->deferToLoop(
      [impl{this->shared_from_this()}]() { impl->closeFromLoop(); });
}

void Channel::Impl::closeFromLoop() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(4) << "Channel " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ChannelClosedError));
}

void Channel::Impl::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

void Channel::Impl::handleError() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(5) << "Channel " << id_ << " is handling error " << error_.what();

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

void Channel::Impl::tryCleanup() {
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

void Channel::Impl::cleanup() {
  TP_DCHECK(context_->inLoop());
  TP_VLOG(8) << "Connection " << id_ << " is cleaning up";

  queuePairs_.clear();
}

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
