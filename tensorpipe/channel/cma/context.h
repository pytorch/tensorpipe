/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/tensor.h>

namespace tensorpipe {
namespace channel {
namespace cma {

class Context : public channel::CpuContext {
 public:
  Context();

  const std::string& domainDescriptor() const override;

  std::shared_ptr<Channel<tensorpipe::CpuTensor>> createChannel(
      std::shared_ptr<transport::Connection>,
      Endpoint) override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  class PrivateIface {
   public:
    virtual ClosingEmitter& getClosingEmitter() = 0;

    using copy_request_callback_fn = std::function<void(const Error&)>;

    virtual void requestCopy(
        pid_t remotePid,
        void* remotePtr,
        void* localPtr,
        size_t length,
        copy_request_callback_fn fn) = 0;

    virtual ~PrivateIface() = default;
  };

  class Impl;

  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  std::shared_ptr<Impl> impl_;

  // Allow channel to see the private interface.
  friend class Channel;
};

} // namespace cma
} // namespace channel
} // namespace tensorpipe
