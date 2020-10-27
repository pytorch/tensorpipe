/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/loop.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/uv/macros.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

Loop::Loop()
    : loop_(std::make_unique<uv_loop_t>()),
      async_(std::make_unique<uv_async_t>()) {
  int rv;
  rv = uv_loop_init(loop_.get());
  TP_THROW_UV_IF(rv < 0, rv);
  rv = uv_async_init(loop_.get(), async_.get(), uv__async_cb);
  TP_THROW_UV_IF(rv < 0, rv);
  async_->data = this;

  startThread("TP_UV_loop");
}

void Loop::close() {
  if (!closed_.exchange(true)) {
    // It's fine to capture this because the loop won't be destroyed until join
    // has completed, and join won't complete until this operation is performed.
    deferToLoop(
        [this]() { uv_unref(reinterpret_cast<uv_handle_t*>(async_.get())); });
  }
}

void Loop::join() {
  close();

  if (!joined_.exchange(true)) {
    joinThread();
  }
}

Loop::~Loop() noexcept {
  join();
}

void Loop::wakeupEventLoopToDeferFunction() {
  auto rv = uv_async_send(async_.get());
  TP_THROW_UV_IF(rv < 0, rv);
}

void Loop::eventLoop() {
  int rv;

  rv = uv_run(loop_.get(), UV_RUN_DEFAULT);
  TP_THROW_ASSERT_IF(rv > 0)
      << ": uv_run returned with active handles or requests";

  uv_ref(reinterpret_cast<uv_handle_t*>(async_.get()));
  uv_close(reinterpret_cast<uv_handle_t*>(async_.get()), nullptr);

  rv = uv_run(loop_.get(), UV_RUN_NOWAIT);
  TP_THROW_ASSERT_IF(rv > 0)
      << ": uv_run returned with active handles or requests";

  // Release resources associated with loop.
  rv = uv_loop_close(loop_.get());
  TP_THROW_UV_IF(rv < 0, rv);
}

void Loop::uv__async_cb(uv_async_t* handle) {
  auto& loop = *reinterpret_cast<Loop*>(handle->data);
  loop.runDeferredFunctionsFromEventLoop();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
