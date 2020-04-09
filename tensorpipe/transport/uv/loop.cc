/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/loop.h>

#include <tensorpipe/transport/uv/macros.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::shared_ptr<Loop> Loop::create() {
  return std::make_shared<Loop>(ConstructorToken());
}

Loop::Loop(ConstructorToken /* unused */)
    : loop_(std::make_unique<uv_loop_t>()),
      async_(std::make_unique<uv_async_t>()) {
  int rv;
  rv = uv_loop_init(loop_.get());
  TP_THROW_UV_IF(rv < 0, rv);
  rv = uv_async_init(loop_.get(), async_.get(), uv__async_cb);
  TP_THROW_UV_IF(rv < 0, rv);
  async_->data = this;
  thread_ = std::thread(&Loop::loop, this);
}

void Loop::close() {
  bool wasClosed = false;
  closed_.compare_exchange_strong(wasClosed, true);
  if (!wasClosed) {
    closingEmitter_.close();
    // It's fine to capture this because the loop won't be destroyed until join
    // has completed, and join won't complete until this operation is performed.
    deferToLoop(
        [this]() { uv_unref(reinterpret_cast<uv_handle_t*>(async_.get())); });
  }
}

void Loop::join() {
  close();

  bool wasJoined = false;
  joined_.compare_exchange_strong(wasJoined, true);
  if (!wasJoined) {
    // Wait for event loop thread to terminate.
    thread_.join();

    // There should not be any pending deferred work at this time.
    TP_DCHECK(fns_.empty());
  }
}

Loop::~Loop() noexcept {
  join();

  // Release resources associated with loop.
  auto rv = uv_loop_close(loop_.get());
  TP_THROW_UV_IF(rv < 0, rv);
}

void Loop::deferToLoop(std::function<void()> fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  fns_.push_back(std::move(fn));
  wakeup();
}

void Loop::wakeup() {
  auto rv = uv_async_send(async_.get());
  TP_THROW_UV_IF(rv < 0, rv);
}

void Loop::loop() {
  int rv;

  rv = uv_run(loop_.get(), UV_RUN_DEFAULT);
  TP_THROW_ASSERT_IF(rv > 0)
      << ": uv_run returned with active handles or requests";

  // We got broken out of the run loop by Loop::join's unref on the
  // async handle. It is possible we still have callbacks to run,
  // which in turn may trigger more work. Therefore, we keep running
  // until the only active handle is the async handle.
  uv_ref(reinterpret_cast<uv_handle_t*>(async_.get()));
  rv = uv_run(loop_.get(), UV_RUN_NOWAIT);
  TP_THROW_ASSERT_IF(rv == 0)
      << ": uv_run returned with no active handles or requests";

  // By this time we expect to have drained all pending work and can
  // safely close the async handle and terminate the thread.
  uv_close(reinterpret_cast<uv_handle_t*>(async_.get()), nullptr);
  rv = uv_run(loop_.get(), UV_RUN_NOWAIT);
  TP_THROW_ASSERT_IF(rv > 0)
      << ": uv_run returned with active handles or requests";
}

void Loop::uv__async_cb(uv_async_t* handle) {
  auto& loop = *reinterpret_cast<Loop*>(handle->data);
  loop.runFunctionsFromLoop();
}

void Loop::runFunctionsFromLoop() {
  decltype(fns_) fns;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    std::swap(fns, fns_);
  }

  for (auto& fn : fns) {
    fn();
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
