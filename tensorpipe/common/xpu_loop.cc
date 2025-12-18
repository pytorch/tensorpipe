/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * Copyright 2025 Intel Corporation.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/xpu_loop.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/common/xpu.h>

namespace tensorpipe {

XpuLoop::XpuLoop() {
  thread_ = std::thread(&XpuLoop::run, this);
}

XpuLoop::~XpuLoop() {
  close();
  join();
}

void XpuLoop::addCallback(
    int deviceIdx,
    sycl::event event,
    std::function<void(const Error&)> fn) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.push_back({deviceIdx, std::move(event), std::move(fn)});
  }
  cv_.notify_one();
}

void XpuLoop::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    done_ = true;
  }
  cv_.notify_all();
}

void XpuLoop::join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

void XpuLoop::run() {
  while (true) {
    std::vector<Op> currentOps;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&] { return done_ || !pending_.empty(); });
      if (done_ && pending_.empty()) {
        return;
      }
      currentOps.swap(pending_);
    }

    for (auto& op : currentOps) {
      try {
        // Wait for the SYCL event to complete asynchronously
        op.event.wait();
        op.fn(Error::kSuccess);
      } catch (const sycl::exception& e) {
        auto err = TP_CREATE_ERROR(xpu::XpuError, e.what());
        op.fn(err);
      }
    }
  }
}

} // namespace tensorpipe
