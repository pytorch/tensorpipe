/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <functional>
#include <thread>

#include <tensorpipe/channel/context_impl_boilerplate.h>
#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/device.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>

namespace tensorpipe {
namespace channel {
namespace xth {

class ChannelImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ChannelImpl> {
 public:
  static std::shared_ptr<ContextImpl> create();

  explicit ContextImpl(
      std::unordered_map<Device, std::string> deviceDescriptors);

  std::shared_ptr<Channel> createChannel(
      std::vector<std::shared_ptr<transport::Connection>> connections,
      Endpoint endpoint);

  size_t numConnectionsNeeded() const override;

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

  using copy_request_callback_fn = std::function<void(const Error&)>;

  void requestCopy(
      void* remotePtr,
      void* localPtr,
      size_t length,
      copy_request_callback_fn fn);

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;

 private:
  OnDemandDeferredExecutor loop_;

  struct CopyRequest {
    void* remotePtr;
    void* localPtr;
    size_t length;
    copy_request_callback_fn callback;
  };

  std::thread thread_;
  Queue<optional<CopyRequest>> requests_;

  // This is atomic because it may be accessed from outside the loop.
  std::atomic<uint64_t> nextRequestId_{0};

  void handleCopyRequests();
};

} // namespace xth
} // namespace channel
} // namespace tensorpipe
