/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

class UVTransportTestHelper : public TransportTestHelper {
 public:
  UVTransportTestHelper() : loop_(tensorpipe::transport::uv::Loop::create()) {}

  ~UVTransportTestHelper() override {
    loop_->join();
  }

  std::shared_ptr<tensorpipe::transport::Listener> getListener() override {
    auto addr =
        tensorpipe::transport::uv::Sockaddr::createInetSockAddr(kIPAddr);
    return tensorpipe::transport::uv::Listener::create(loop_, addr);
  }

  std::shared_ptr<tensorpipe::transport::Connection> connect(
      const std::string& addr) override {
    auto saddr = tensorpipe::transport::uv::Sockaddr::createInetSockAddr(addr);
    return tensorpipe::transport::uv::Connection::create(loop_, saddr);
  }

  static std::string transportName() {
    return "uv";
  }

 private:
  const std::string kIPAddr = "127.0.0.1";
  std::shared_ptr<tensorpipe::transport::uv::Loop> loop_;
};
