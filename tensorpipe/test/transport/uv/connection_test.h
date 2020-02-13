/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/test/transport/connection_test.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

class UVConnectionTestHelper : public ConnectionTestHelper {
 public:
  UVConnectionTestHelper() : loop_(tensorpipe::transport::uv::Loop::create()) {}

  ~UVConnectionTestHelper() override {
    loop_->join();
  }

  std::shared_ptr<tensorpipe::transport::Listener> getListener() override {
    using namespace tensorpipe::transport;
    auto addr = uv::Sockaddr::createInetSockAddr(kIPAddr);
    return uv::Listener::create(loop_, addr);
  }

  std::shared_ptr<tensorpipe::transport::Connection> connect(
      const std::string& addr) override {
    using namespace tensorpipe::transport;
    auto saddr = uv::Sockaddr::createInetSockAddr(addr);
    return uv::Connection::create(loop_, saddr);
  }

  static std::string transportName() {
    return "uv";
  }

 private:
  const std::string kIPAddr = "127.0.0.1";
  std::shared_ptr<tensorpipe::transport::uv::Loop> loop_;
};
