/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Loop;

class Context final : public transport::Context {
 public:
  explicit Context();

  ~Context() override;

  void join() override;

  std::shared_ptr<transport::Connection> connect(address_t addr) override;

  std::shared_ptr<transport::Listener> listen(address_t addr) override;

  const std::string& domainDescriptor() const override;

 private:
  std::shared_ptr<Loop> loop_;
  std::string domainDescriptor_;
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
