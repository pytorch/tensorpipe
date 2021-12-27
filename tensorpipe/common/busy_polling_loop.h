/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <utility>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {

class BusyPollingLoop : public EventLoopDeferredExecutor {
 protected:
  virtual bool pollOnce() = 0;

  virtual bool readyToClose() = 0;

  void stopBusyPolling() {
    closed_ = true;
    // No need to wake up the thread, since it is busy-waiting.
  }

  void eventLoop() override {
    while (!closed_ || !readyToClose()) {
      if (pollOnce()) {
        // continue
      } else if (deferredFunctionCount_ > 0) {
        deferredFunctionCount_ -= runDeferredFunctionsFromEventLoop();
      } else {
        std::this_thread::yield();
      }
    }
  }

  void wakeupEventLoopToDeferFunction() override {
    ++deferredFunctionCount_;
    // No need to wake up the thread, since it is busy-waiting.
  }

 private:
  std::atomic<bool> closed_{false};

  std::atomic<int64_t> deferredFunctionCount_{0};
};

} // namespace tensorpipe
