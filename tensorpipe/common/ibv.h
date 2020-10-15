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

std::string ibvWorkCompletionOpcodeToStr(enum ibv_wc_opcode opcode);

// RAII wrappers

class IbvDeviceList {
 public:
  IbvDeviceList()
      : deviceList_(TP_CHECK_IBV_PTR(ibv_get_device_list(&size_))) {}

  int size() {
    return size_;
  }

  struct ibv_device& operator[](int i) {
    return *deviceList_.get()[i];
  }

  void reset() {
    deviceList_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_device** ptr) const {
      TP_CHECK_IBV_VOID(ibv_free_device_list(ptr));
    }
  };

  std::unique_ptr<struct ibv_device*, Deleter> deviceList_;
  int size_;
};

struct IbvContextDeleter {
  void operator()(struct ibv_context* ptr) {
    TP_CHECK_IBV_INT(ibv_close_device(ptr));
  }
};

using IbvContext = std::unique_ptr<struct ibv_context, IbvContextDeleter>;

inline IbvContext createIbvContext(struct ibv_device& device) {
  return IbvContext(TP_CHECK_IBV_PTR(ibv_open_device(&device)));
}

struct IbvProtectionDomainDeleter {
  void operator()(struct ibv_pd* ptr) {
    TP_CHECK_IBV_INT(ibv_dealloc_pd(ptr));
  }
};

using IbvProtectionDomain =
    std::unique_ptr<struct ibv_pd, IbvProtectionDomainDeleter>;

inline IbvProtectionDomain createIbvProtectionDomain(IbvContext& context) {
  return IbvProtectionDomain(TP_CHECK_IBV_PTR(ibv_alloc_pd(context.get())));
}

struct IbvCompletionQueueDeleter {
  void operator()(struct ibv_cq* ptr) {
    TP_CHECK_IBV_INT(ibv_destroy_cq(ptr));
  }
};

using IbvCompletionQueue =
    std::unique_ptr<struct ibv_cq, IbvCompletionQueueDeleter>;

inline IbvCompletionQueue createIbvCompletionQueue(
    IbvContext& context,
    int cqe,
    void* cq_context,
    struct ibv_comp_channel* channel,
    int comp_vector) {
  return IbvCompletionQueue(TP_CHECK_IBV_PTR(
      ibv_create_cq(context.get(), cqe, cq_context, channel, comp_vector)));
}

struct IbvSharedReceiveQueueDeleter {
  void operator()(struct ibv_srq* ptr) {
    TP_CHECK_IBV_INT(ibv_destroy_srq(ptr));
  }
};

using IbvSharedReceiveQueue =
    std::unique_ptr<struct ibv_srq, IbvSharedReceiveQueueDeleter>;

inline IbvSharedReceiveQueue createIbvSharedReceiveQueue(
    IbvProtectionDomain& pd,
    struct ibv_srq_init_attr& initAttr) {
  return IbvSharedReceiveQueue(
      TP_CHECK_IBV_PTR(ibv_create_srq(pd.get(), &initAttr)));
}

struct IbvMemoryRegionDeleter {
  void operator()(struct ibv_mr* ptr) {
    TP_CHECK_IBV_INT(ibv_dereg_mr(ptr));
  }
};

using IbvMemoryRegion = std::unique_ptr<struct ibv_mr, IbvMemoryRegionDeleter>;

inline IbvMemoryRegion createIbvMemoryRegion(
    IbvProtectionDomain& pd,
    void* addr,
    size_t length,
    int accessFlags) {
  return IbvMemoryRegion(
      TP_CHECK_IBV_PTR(ibv_reg_mr(pd.get(), addr, length, accessFlags)));
}

struct IbvQueuePairDeleter {
  void operator()(struct ibv_qp* ptr) {
    TP_CHECK_IBV_INT(ibv_destroy_qp(ptr));
  }
};

using IbvQueuePair = std::unique_ptr<struct ibv_qp, IbvQueuePairDeleter>;

inline IbvQueuePair createIbvQueuePair(
    IbvProtectionDomain& pd,
    struct ibv_qp_init_attr& initAttr) {
  return IbvQueuePair(TP_CHECK_IBV_PTR(ibv_create_qp(pd.get(), &initAttr)));
}

// Helpers

struct IbvAddress {
  uint8_t portNum;
  uint8_t globalIdentifierIndex;
  // The already-resolved LID of the above device+port pair.
  uint32_t localIdentifier;
  // The already-resolved GID of the above device+port+index combination.
  union ibv_gid globalIdentifier;
  enum ibv_mtu maximumTransmissionUnit;
};

struct IbvSetupInformation {
  uint32_t localIdentifier;
  union ibv_gid globalIdentifier;
  uint32_t queuePairNumber;
  enum ibv_mtu maximumTransmissionUnit;
};

struct IbvAddress makeIbvAddress(
    IbvContext& context,
    uint8_t portNum,
    uint8_t globalIdentifierIndex);

struct IbvSetupInformation makeIbvSetupInformation(
    IbvAddress& addr,
    IbvQueuePair& qp);

void transitionIbvQueuePairToInit(IbvQueuePair& qp, IbvAddress& selfAddr);

void transitionIbvQueuePairToReadyToReceive(
    IbvQueuePair& qp,
    IbvAddress& selfAddr,
    IbvSetupInformation& destinationInfo);

void transitionIbvQueuePairToReadyToSend(
    IbvQueuePair& qp,
    IbvSetupInformation& selfInfo);

void transitionIbvQueuePairToError(IbvQueuePair& qp);

} // namespace tensorpipe
