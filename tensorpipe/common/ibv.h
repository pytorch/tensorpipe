/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <infiniband/verbs.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/ibv_lib.h>

namespace tensorpipe {

// Error checking macros

#define TP_CHECK_IBV_PTR(op)                   \
  [&]() {                                      \
    auto ptr = op;                             \
    TP_THROW_SYSTEM_IF(ptr == nullptr, errno); \
    return ptr;                                \
  }()

#define TP_CHECK_IBV_INT(op)           \
  {                                    \
    int rv = op;                       \
    TP_THROW_SYSTEM_IF(rv < 0, errno); \
  }

#define TP_CHECK_IBV_VOID(op) op;

// Logging helpers

std::string ibvWorkCompletionOpcodeToStr(IbvLib::wc_opcode opcode);

// RAII wrappers

class IbvDeviceList {
 public:
  IbvDeviceList() = default;

  explicit IbvDeviceList(IbvLib& ibvLib)
      : deviceList_(
            TP_CHECK_IBV_PTR(ibvLib.get_device_list(&size_)),
            Deleter{&ibvLib}) {}

  int size() {
    return size_;
  }

  IbvLib::device& operator[](int i) {
    return *deviceList_.get()[i];
  }

  void reset() {
    deviceList_.reset();
  }

 private:
  struct Deleter {
    void operator()(IbvLib::device** ptr) {
      TP_CHECK_IBV_VOID(ibvLib->free_device_list(ptr));
    }

    IbvLib* ibvLib;
  };

  std::unique_ptr<IbvLib::device*, Deleter> deviceList_;
  int size_;
};

struct IbvContextDeleter {
  void operator()(IbvLib::context* ptr) {
    TP_CHECK_IBV_INT(ibvLib->close_device(ptr));
  }

  IbvLib* ibvLib;
};

using IbvContext = std::unique_ptr<IbvLib::context, IbvContextDeleter>;

inline IbvContext createIbvContext(IbvLib& ibvLib, IbvLib::device& device) {
  return IbvContext(
      TP_CHECK_IBV_PTR(ibvLib.open_device(&device)),
      IbvContextDeleter{&ibvLib});
}

struct IbvProtectionDomainDeleter {
  void operator()(IbvLib::pd* ptr) {
    TP_CHECK_IBV_INT(ibvLib->dealloc_pd(ptr));
  }

  IbvLib* ibvLib;
};

using IbvProtectionDomain =
    std::unique_ptr<IbvLib::pd, IbvProtectionDomainDeleter>;

inline IbvProtectionDomain createIbvProtectionDomain(
    IbvLib& ibvLib,
    IbvContext& context) {
  return IbvProtectionDomain(
      TP_CHECK_IBV_PTR(ibvLib.alloc_pd(context.get())),
      IbvProtectionDomainDeleter{&ibvLib});
}

struct IbvCompletionQueueDeleter {
  void operator()(IbvLib::cq* ptr) {
    TP_CHECK_IBV_INT(ibvLib->destroy_cq(ptr));
  }

  IbvLib* ibvLib;
};

using IbvCompletionQueue =
    std::unique_ptr<IbvLib::cq, IbvCompletionQueueDeleter>;

inline IbvCompletionQueue createIbvCompletionQueue(
    IbvLib& ibvLib,
    IbvContext& context,
    int cqe,
    void* cq_context,
    IbvLib::comp_channel* channel,
    int comp_vector) {
  return IbvCompletionQueue(
      TP_CHECK_IBV_PTR(ibvLib.create_cq(
          context.get(), cqe, cq_context, channel, comp_vector)),
      IbvCompletionQueueDeleter{&ibvLib});
}

struct IbvSharedReceiveQueueDeleter {
  void operator()(IbvLib::srq* ptr) {
    TP_CHECK_IBV_INT(ibvLib->destroy_srq(ptr));
  }

  IbvLib* ibvLib;
};

using IbvSharedReceiveQueue =
    std::unique_ptr<IbvLib::srq, IbvSharedReceiveQueueDeleter>;

inline IbvSharedReceiveQueue createIbvSharedReceiveQueue(
    IbvLib& ibvLib,
    IbvProtectionDomain& pd,
    IbvLib::srq_init_attr& initAttr) {
  return IbvSharedReceiveQueue(
      TP_CHECK_IBV_PTR(ibvLib.create_srq(pd.get(), &initAttr)),
      IbvSharedReceiveQueueDeleter{&ibvLib});
}

struct IbvMemoryRegionDeleter {
  void operator()(IbvLib::mr* ptr) {
    TP_CHECK_IBV_INT(ibvLib->dereg_mr(ptr));
  }

  IbvLib* ibvLib;
};

using IbvMemoryRegion = std::unique_ptr<IbvLib::mr, IbvMemoryRegionDeleter>;

inline IbvMemoryRegion createIbvMemoryRegion(
    IbvLib& ibvLib,
    IbvProtectionDomain& pd,
    void* addr,
    size_t length,
    int accessFlags) {
  return IbvMemoryRegion(
      TP_CHECK_IBV_PTR(ibvLib.reg_mr(pd.get(), addr, length, accessFlags)),
      IbvMemoryRegionDeleter{&ibvLib});
}

struct IbvQueuePairDeleter {
  void operator()(IbvLib::qp* ptr) {
    TP_CHECK_IBV_INT(ibvLib->destroy_qp(ptr));
  }

  IbvLib* ibvLib;
};

using IbvQueuePair = std::unique_ptr<IbvLib::qp, IbvQueuePairDeleter>;

inline IbvQueuePair createIbvQueuePair(
    IbvLib& ibvLib,
    IbvProtectionDomain& pd,
    IbvLib::qp_init_attr& initAttr) {
  return IbvQueuePair(
      TP_CHECK_IBV_PTR(ibvLib.create_qp(pd.get(), &initAttr)),
      IbvQueuePairDeleter{&ibvLib});
}

// Helpers

struct IbvAddress {
  uint8_t portNum;
  uint8_t globalIdentifierIndex;
  // The already-resolved LID of the above device+port pair.
  uint32_t localIdentifier;
  // The already-resolved GID of the above device+port+index combination.
  IbvLib::gid globalIdentifier;
  IbvLib::mtu maximumTransmissionUnit;
};

struct IbvSetupInformation {
  uint32_t localIdentifier;
  IbvLib::gid globalIdentifier;
  uint32_t queuePairNumber;
  IbvLib::mtu maximumTransmissionUnit;
};

struct IbvAddress makeIbvAddress(
    IbvLib& ibvLib,
    IbvContext& context,
    uint8_t portNum,
    uint8_t globalIdentifierIndex);

struct IbvSetupInformation makeIbvSetupInformation(
    IbvAddress& addr,
    IbvQueuePair& qp);

void transitionIbvQueuePairToInit(
    IbvLib& ibvLib,
    IbvQueuePair& qp,
    IbvAddress& selfAddr);

void transitionIbvQueuePairToReadyToReceive(
    IbvLib& ibvLib,
    IbvQueuePair& qp,
    IbvAddress& selfAddr,
    IbvSetupInformation& destinationInfo);

void transitionIbvQueuePairToReadyToSend(
    IbvLib& ibvLib,
    IbvQueuePair& qp,
    IbvSetupInformation& selfInfo);

void transitionIbvQueuePairToError(IbvLib& ibvLib, IbvQueuePair& qp);

} // namespace tensorpipe
