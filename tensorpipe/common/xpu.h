/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <sycl/sycl.hpp>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/strings.h>

namespace tensorpipe {
namespace xpu {

class XpuError final : public BaseError {
 public:
  explicit XpuError(const sycl::exception& e) : msg_(e.what()) {}
  explicit XpuError(std::string msg) : msg_(std::move(msg)) {}

  std::string what() const override {
    return msg_;
  }

 private:
  std::string msg_;
};

class XpuEvent {
 public:
  XpuEvent() = default;

  inline bool isCreated() const {
    return (event_.get() != nullptr);
  }

  void record(sycl::queue& q) {
    if (!isCreated()) {
      event_ = std::make_unique<sycl::event>(q.ext_oneapi_submit_barrier());
    } else {
      event_.reset();
      event_ = std::make_unique<sycl::event>(q.ext_oneapi_submit_barrier());
    }
  }

  void synchronize() {
    if (isCreated()) {
      event().wait_and_throw();
    }
  }

  bool query() const {
    if (!isCreated())
      return true;
    return event().get_info<sycl::info::event::command_execution_status>() ==
        sycl::info::event_command_status::complete;
  }

  void wait(sycl::queue& q) {
    if (isCreated()) {
      q.ext_oneapi_submit_barrier({event()});
    }
  }

  sycl::event& event() const {
    return *event_;
  }

 private:
  std::unique_ptr<sycl::event> event_;
  sycl::queue q;
};

inline std::vector<Device> getXpuDevices() {
  // Enumerate available SYCL GPU devices
  auto devices = sycl::device::get_devices(sycl::info::device_type::gpu);

  std::vector<Device> result;
  result.reserve(devices.size());

  for (size_t devIdx = 0; devIdx < devices.size(); ++devIdx) {
    result.push_back(Device{kXpuDeviceType, static_cast<int>(devIdx)});
  }

  return result;
}

inline int xpuDeviceForPointer(const void* ptr) {
  TP_THROW_ASSERT_IF(ptr == nullptr) << "Pointer is null";

  // Get all GPU devices
  auto devices = sycl::device::get_devices(sycl::info::device_type::gpu);

  // Create contexts once for all devices
  std::vector<sycl::context> contexts;
  contexts.reserve(devices.size());
  for (const auto& dev : devices) {
    contexts.emplace_back(dev);
  }

  // Check which context owns the pointer
  for (int i = 0; i < contexts.size(); i++) {
    auto type = sycl::get_pointer_type(ptr, contexts[i]);
    if (type != sycl::usm::alloc::unknown) {
      // verify device ownership
      auto dev = sycl::get_pointer_device(ptr, contexts[i]);
      if (dev == devices[i]) {
        return i;
      }
    }
  }

  return -1;
}

class XpuPinnedMemoryDeleter {
 public:
  explicit XpuPinnedMemoryDeleter(sycl::queue* queue) : q_(queue) {}

  void operator()(uint8_t* ptr) const {
    sycl::free(ptr, *q_);
  }

 private:
  sycl::queue* q_;
};

using XpuPinnedBuffer = std::unique_ptr<uint8_t[], XpuPinnedMemoryDeleter>;

inline XpuPinnedBuffer makeXpuPinnedBuffer(size_t length, sycl::queue& q) {
  uint8_t* ptr = static_cast<uint8_t*>(sycl::malloc_host(length, q));
  TP_THROW_ASSERT_IF(ptr == nullptr) << "malloc_host failed";

  return XpuPinnedBuffer(ptr, XpuPinnedMemoryDeleter(&q));
}

class XpuDeviceBuffer {
 public:
  XpuDeviceBuffer() = default;

  XpuDeviceBuffer(size_t length, sycl::queue& q) : q_(&q) {
    uint8_t* ptr = static_cast<uint8_t*>(sycl::malloc_device(length, q));
    TP_THROW_ASSERT_IF(ptr == nullptr) << "malloc_device failed";

    ptr_ = {ptr, Deleter{q_}};
  }

  uint8_t* ptr() const {
    return ptr_.get();
  }

  void reset() {
    ptr_.reset();
  }

 private:
  struct Deleter {
    sycl::queue* q_;

    void operator()(uint8_t* ptr) {
      sycl::free(ptr, *q_);
    }
  };

  std::unique_ptr<uint8_t[], Deleter> ptr_;
  sycl::queue* q_{nullptr};
};

inline sycl::queue& getDefaultXPUQueue(int deviceIndex) {
  static std::once_flag initFlag;
  static std::vector<std::unique_ptr<sycl::queue>> queues;

  std::call_once(initFlag, []() {
    auto devs = sycl::device::get_devices(sycl::info::device_type::gpu);
    queues.resize(devs.size());

    for (size_t i = 0; i < devs.size(); i++) {
      queues[i] = std::make_unique<sycl::queue>(devs[i]);
    }
  });
  return *queues[deviceIndex];
}

} // namespace xpu
} // namespace tensorpipe
