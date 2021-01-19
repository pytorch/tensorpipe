/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class ContextImpl;

class Context : public transport::Context {
 public:
  Context();

  Context(const Context&) = delete;
  Context(Context&&) = delete;
  Context& operator=(const Context&) = delete;
  Context& operator=(Context&&) = delete;

  std::shared_ptr<Connection> connect(std::string addr) override;

  std::shared_ptr<Listener> listen(std::string addr) override;

  bool isViable() const override;

  const std::string& domainDescriptor() const override;

  void setId(std::string id) override;

  void close() override;

  void join() override;

  ~Context() override;

 private:
  // The implementation is managed by a shared_ptr because each child object
  // will also hold a shared_ptr to it (downcast as a shared_ptr to the private
  // interface). However, its lifetime is tied to the one of this public object,
  // since when the latter is destroyed the implementation is closed and joined.
  const std::shared_ptr<ContextImpl> impl_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
