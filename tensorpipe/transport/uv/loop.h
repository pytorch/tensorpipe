/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <uv.h>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Loop final : public EventLoopDeferredExecutor {
 public:
  Loop();

  uv_loop_t* ptr() {
    return &loop_;
  }

  bool closed() {
    return closed_;
  }

  void close();

  void join();

  ~Loop() noexcept;

 protected:
  // Event loop thread entry function.
  void eventLoop() override;

  // Clean up after event loop transitioned to on-demand.
  void cleanUpLoop() override;

  // Wake up the event loop.
  void wakeupEventLoopToDeferFunction() override;

 private:
  uv_loop_t loop_;
  uv_async_t async_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  // This function is called by the event loop thread whenever
  // we have to run a number of deferred functions.
  static void uvAsyncCb(uv_async_t* handle);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
