/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Loop final : public EventLoopDeferredExecutor {
 public:
  Loop();

  uv_loop_t* ptr() {
    return &loop_;
  }

  void close();

  void join();

  ~Loop() noexcept;

 protected:
  // Event loop thread entry function.
  void eventLoop() override;

  // Wake up the event loop.
  void wakeupEventLoopToDeferFunction() override;

 private:
  uv_loop_t loop_;
  uv_async_t async_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  // This function is called by the event loop thread whenever
  // we have to run a number of deferred functions.
  static void uv__async_cb(uv_async_t* handle);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
