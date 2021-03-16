/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

class CudaEventPoolClosedError final : public BaseError {
  std::string what() const override {
    return "CUDA event pool closed";
  }
};

struct CudaEventSyncDeleter {
  void operator()(optional<CudaEvent>* ptr) {
    TP_THROW_ASSERT_IF(deviceIdx < 0);
    // It's unclear what causes the deadlock with CUDA IPC events, if it's the
    // creation or the destruction step. The deadlock tends to manifest as a
    // cudaStreamSynchronize call never returning. Just to be safe, and to catch
    // such a deadlock early and clearly, let's add extra syncs here.
    {
      CudaDeviceGuard guard(deviceIdx);
      TP_CUDA_CHECK(cudaDeviceSynchronize());
    }
    std::default_delete<optional<CudaEvent>[]>()(ptr);
    {
      CudaDeviceGuard guard(deviceIdx);
      TP_CUDA_CHECK(cudaDeviceSynchronize());
    }
  }

  int deviceIdx{-1};
};

inline std::unique_ptr<optional<CudaEvent>[], CudaEventSyncDeleter> createEvents(
    int numEvents,
    int deviceIdx,
    bool interprocess = false) {
  // It's unclear what causes the deadlock with CUDA IPC events, if it's the
  // creation or the destruction step. The deadlock tends to manifest as a
  // cudaStreamSynchronize call never returning. Just to be safe, and to catch
  // such a deadlock early and clearly, let's add extra syncs here.
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
  auto events = std::make_unique<optional<CudaEvent>[]>(numEvents);
  for (size_t eventIdx = 0; eventIdx < numEvents; eventIdx++) {
    events[eventIdx].emplace(deviceIdx, interprocess);
  }
  {
    CudaDeviceGuard guard(deviceIdx);
    TP_CUDA_CHECK(cudaDeviceSynchronize());
  }
  // Swap out the default deleter with our custom one.
  return std::unique_ptr<optional<CudaEvent>[], CudaEventSyncDeleter>(
      events.release(), CudaEventSyncDeleter{deviceIdx});
}

class CudaEventPool {
 public:
  struct BorrowedEventDeleter {
    void operator()(CudaEvent* ptr) {
      pool.returnPtr(ptr);
    }

    CudaEventPool& pool;
  };

  // This should be a std::unique_ptr<CudaEvent, BorrowedEventDeleter>, but this
  // is captured in a lambda by CallbackWrapper, and that lambda is then wrapped
  // in a std::function, hence we need a copyable smart pointer type here.
  // FIXME Fix this once we replace std::function with a better type.
  using BorrowedEvent = std::shared_ptr<CudaEvent>;
  using RequestCallback = std::function<void(const Error&, BorrowedEvent)>;

  CudaEventPool(size_t numEvents, int deviceIdx, bool interprocess = false)
      : events_(createEvents(numEvents, deviceIdx, interprocess)) {
    for (size_t eventIdx = 0; eventIdx < numEvents; eventIdx++) {
      // One day we might get tempted to have CudaEvent lazily initialize its
      // cudaEvent_t, just like PyTorch does. However this completely defeats
      // the purpose of this class, which is to eagerly initialize IPC events,
      // as creating them late might deadlock with old CUDA driver versions.
      // This check should hopefully catch if the CudaEvent is lazy-initialized.
      TP_THROW_ASSERT_IF(events_[eventIdx]->raw() == nullptr);
      availableEvents_.push_back(&events_[eventIdx].value());
    }
  }

  void request(RequestCallback callback) {
    if (closed_) {
      callback(
          TP_CREATE_ERROR(CudaEventPoolClosedError),
          BorrowedEvent(nullptr, BorrowedEventDeleter{*this}));
      return;
    }
    if (!availableEvents_.empty()) {
      callback(
          Error::kSuccess,
          BorrowedEvent(availableEvents_.front(), BorrowedEventDeleter{*this}));
      availableEvents_.pop_front();
      return;
    }
    pendingCallbacks_.push_back(std::move(callback));
  }

  void close() {
    closed_ = true;
    for (auto& callback : pendingCallbacks_) {
      callback(
          TP_CREATE_ERROR(CudaEventPoolClosedError),
          BorrowedEvent(nullptr, BorrowedEventDeleter{*this}));
    }
    pendingCallbacks_.clear();
  }

 private:
  bool closed_{false};
  // FIXME We'd want this to be a std::unique_ptr<CudaEvent[]>, but CudaEvents
  // aren't default-constructible nor movable. Hence either we make them such,
  // or we use some pointer magic (like placement new). For now, we work around
  // this by wrapping them in optional<>, but it's silly.
  const std::unique_ptr<optional<CudaEvent>[], CudaEventSyncDeleter> events_;
  std::deque<CudaEvent*> availableEvents_;
  std::deque<RequestCallback> pendingCallbacks_;

  void returnPtr(CudaEvent* ptr) {
    TP_DCHECK(ptr != nullptr);
    if (!pendingCallbacks_.empty()) {
      TP_DCHECK(!closed_);
      pendingCallbacks_.front()(
          Error::kSuccess, BorrowedEvent(ptr, BorrowedEventDeleter{*this}));
      pendingCallbacks_.pop_front();
      return;
    }
    availableEvents_.push_back(ptr);
  }
};

} // namespace tensorpipe
