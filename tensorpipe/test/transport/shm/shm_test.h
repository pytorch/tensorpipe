/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/test/transport/transport_test.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>

class SHMTransportTestHelper : public TransportTestHelper {
 public:
  SHMTransportTestHelper()
      : loop_(tensorpipe::transport::shm::Loop::create()) {}

  ~SHMTransportTestHelper() override {
    loop_->join();
  }

  std::shared_ptr<tensorpipe::transport::Listener> getListener() override {
    auto addr =
        tensorpipe::transport::shm::Sockaddr::createAbstractUnixAddr(kUnixAddr);
    return tensorpipe::transport::shm::Listener::create(loop_, addr);
  }

  std::shared_ptr<tensorpipe::transport::Connection> connect(
      const std::string& addr) override {
    auto socket = tensorpipe::transport::shm::Socket::createForFamily(AF_UNIX);
    auto saddr =
        tensorpipe::transport::shm::Sockaddr::createAbstractUnixAddr(addr);
    socket->connect(saddr);
    return tensorpipe::transport::shm::Connection::create(
        loop_, std::move(socket));
  }

  static std::string transportName() {
    return "shm";
  }

 private:
  const std::string kUnixAddr = "foobar";
  std::shared_ptr<tensorpipe::transport::shm::Loop> loop_;
};
