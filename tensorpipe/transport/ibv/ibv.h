/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <infiniband/verbs.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

// Error checking macros

#define TP_CHECK_IBV_PTR(op)                   \
  [&]() {                                      \
    auto ptr = op;                             \
    TP_THROW_SYSTEM_IF(ptr == nullptr, errno); \
    return ptr;                                \
  }()

#define TP_CHECK_IBV_INT(op)            \
  {                                     \
    int rv = op;                        \
    TP_THROW_SYSTEM_IF(rv != 0, errno); \
  }

#define TP_CHECK_IBV_VOID(op) op;

// RAII wrappers

class IbvDeviceList {
 public:
  IbvDeviceList() {
    deviceList_ =
        decltype(deviceList_)(TP_CHECK_IBV_PTR(ibv_get_device_list(&size_)));
  }

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

class IbvContext {
 public:
  IbvContext() = default;

  explicit IbvContext(struct ibv_device& device) {
    context_ = decltype(context_)(TP_CHECK_IBV_PTR(ibv_open_device(&device)));
  }

  struct ibv_context* ptr() {
    return context_.get();
  }

  struct ibv_context* operator->() {
    return ptr();
  }

  void reset() {
    context_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_context* ptr) {
      TP_CHECK_IBV_INT(ibv_close_device(ptr));
    }
  };

  std::unique_ptr<struct ibv_context, Deleter> context_;
};

class IbvProtectionDomain {
 public:
  IbvProtectionDomain() = default;

  explicit IbvProtectionDomain(IbvContext& context) {
    protectionDomain_ = decltype(protectionDomain_)(
        TP_CHECK_IBV_PTR(ibv_alloc_pd(context.ptr())));
  }

  struct ibv_pd* ptr() {
    return protectionDomain_.get();
  }

  struct ibv_pd* operator->() {
    return ptr();
  }

  void reset() {
    protectionDomain_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_pd* ptr) {
      TP_CHECK_IBV_INT(ibv_dealloc_pd(ptr));
    }
  };

  std::unique_ptr<struct ibv_pd, Deleter> protectionDomain_;
};

class IbvCompletionQueue {
 public:
  IbvCompletionQueue() = default;

  IbvCompletionQueue(
      IbvContext& context,
      int cqe,
      void* cq_context,
      struct ibv_comp_channel* channel,
      int comp_vector) {
    completionQueue_ = decltype(completionQueue_)(TP_CHECK_IBV_PTR(
        ibv_create_cq(context.ptr(), cqe, cq_context, channel, comp_vector)));
  }

  struct ibv_cq* ptr() {
    return completionQueue_.get();
  }

  struct ibv_cq* operator->() {
    return ptr();
  }

  void reset() {
    completionQueue_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_cq* ptr) {
      TP_CHECK_IBV_INT(ibv_destroy_cq(ptr));
    }
  };

  std::unique_ptr<struct ibv_cq, Deleter> completionQueue_;
};

class IbvSharedReceiveQueue {
 public:
  IbvSharedReceiveQueue() = default;

  IbvSharedReceiveQueue(
      IbvProtectionDomain& pd,
      struct ibv_srq_init_attr& initAttr) {
    sharedReceiveQueue_ = decltype(sharedReceiveQueue_)(
        TP_CHECK_IBV_PTR(ibv_create_srq(pd.ptr(), &initAttr)));
  }

  struct ibv_srq* ptr() {
    return sharedReceiveQueue_.get();
  }

  struct ibv_srq* operator->() {
    return ptr();
  }

  void reset() {
    sharedReceiveQueue_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_srq* ptr) {
      TP_CHECK_IBV_INT(ibv_destroy_srq(ptr));
    }
  };

  std::unique_ptr<struct ibv_srq, Deleter> sharedReceiveQueue_;
};

class IbvMemoryRegion {
 public:
  IbvMemoryRegion() = default;

  IbvMemoryRegion(
      IbvProtectionDomain& pd,
      void* addr,
      size_t length,
      int accessFlags) {
    memoryRegion_ = decltype(memoryRegion_)(
        TP_CHECK_IBV_PTR(ibv_reg_mr(pd.ptr(), addr, length, accessFlags)));
  }

  struct ibv_mr* ptr() {
    return memoryRegion_.get();
  }

  struct ibv_mr* operator->() {
    return ptr();
  }

  void reset() {
    memoryRegion_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_mr* ptr) {
      TP_CHECK_IBV_INT(ibv_dereg_mr(ptr));
    }
  };

  std::unique_ptr<struct ibv_mr, Deleter> memoryRegion_;
};

class IbvQueuePair {
 public:
  IbvQueuePair() = default;

  IbvQueuePair(IbvProtectionDomain& pd, struct ibv_qp_init_attr& initAttr) {
    queuePair_ = decltype(queuePair_)(
        TP_CHECK_IBV_PTR(ibv_create_qp(pd.ptr(), &initAttr)));
  }

  struct ibv_qp* ptr() {
    return queuePair_.get();
  }

  struct ibv_qp* operator->() {
    return ptr();
  }

  void reset() {
    queuePair_.reset();
  }

 private:
  struct Deleter {
    void operator()(struct ibv_qp* ptr) {
      TP_CHECK_IBV_INT(ibv_destroy_qp(ptr));
    }
  };

  std::unique_ptr<struct ibv_qp, Deleter> queuePair_;
};

// Helpers

struct IbvAddress {
  uint8_t portNum;
  uint8_t globalIdentifierIndex;
  // The already-resolved LID of the above device+port pair.
  uint32_t localIdentifier;
  // The already-resolved GID of the above device+port+index combination.
  union ibv_gid globalIdentifier;
};

struct IbvSetupInformation {
  uint32_t localIdentifier;
  union ibv_gid globalIdentifier;
  uint32_t queuePairNumber;
  uint32_t packetSequenceNumber;
};

struct IbvAddress makeIbvAddress(
    IbvContext& context,
    uint8_t portNum,
    uint8_t globalIdentifierIndex);

struct IbvSetupInformation makeIbvSetupInformation(
    IbvAddress& addr,
    IbvQueuePair& qp);

IbvQueuePair createIbvQueuePair(
    IbvProtectionDomain& pd,
    IbvCompletionQueue& cq,
    IbvSharedReceiveQueue& srq);

void transitionIbvQueuePairToInit(IbvQueuePair& qp, IbvAddress& selfAddr);

void transitionIbvQueuePairToReadyToReceive(
    IbvQueuePair& qp,
    IbvAddress& selfAddr,
    IbvSetupInformation& destinationInfo);

void transitionIbvQueuePairToReadyToSend(
    IbvQueuePair& qp,
    IbvSetupInformation& selfInfo);

void transitionIbvQueuePairToError(IbvQueuePair& qp);

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
