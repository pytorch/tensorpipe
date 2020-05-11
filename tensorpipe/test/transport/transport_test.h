/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <thread>

#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/listener.h>

#include <gtest/gtest.h>

class TransportTestHelper {
 public:
  virtual std::shared_ptr<tensorpipe::transport::Context> getContext() = 0;

  virtual std::string defaultAddr() = 0;

  virtual ~TransportTestHelper() = default;
};

class TransportTest : public ::testing::TestWithParam<TransportTestHelper*> {
 public:
  void testConnection(
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          listeningFn,
      std::function<void(std::shared_ptr<tensorpipe::transport::Connection>)>
          connectingFn) {
    using namespace tensorpipe::transport;

    auto ctx = GetParam()->getContext();
    auto addr = GetParam()->defaultAddr();
    {
      auto listener = ctx->listen(addr);
      tensorpipe::Queue<std::shared_ptr<Connection>> queue;
      listener->accept([&](const tensorpipe::Error& error,
                           std::shared_ptr<Connection> conn) {
        ASSERT_FALSE(error) << error.what();
        queue.push(std::move(conn));
      });

      // Start thread for listening side.
      std::thread listeningThread([&]() { listeningFn(queue.pop()); });

      // Capture real listener address.
      const std::string listenerAddr = listener->addr();

      // Start thread for connecting side.
      std::thread connectingThread(
          [&]() { connectingFn(ctx->connect(listenerAddr)); });

      // Wait for completion.
      listeningThread.join();
      connectingThread.join();
    }

    ctx->join();
  }

  // Add to a closure to check the callback is called before being destroyed
  class Bomb {
   public:
    Bomb() = default;

    Bomb(const Bomb&) = delete;
    Bomb(Bomb&& b) {
      defused_ = b.defused_;
      b.defused_ = false;
    }

    Bomb& operator=(const Bomb&) = delete;
    Bomb& operator=(Bomb&&) = delete;

    void defuse() {
      defused_ = true;
    }

    ~Bomb() {
      EXPECT_TRUE(defused_);
    }

   private:
    bool defused_ = false;
  };

  std::shared_ptr<Bomb> armBomb() {
    return std::make_shared<Bomb>();
  }

  void doRead(
      std::shared_ptr<tensorpipe::transport::Connection> conn,
      tensorpipe::transport::Connection::read_callback_fn fn) {
    auto mutex = std::make_shared<std::mutex>();
    std::lock_guard<std::mutex> outerLock(*mutex);
    // We acquire the same mutex while calling read and inside its callback so
    // that we deadlock if the callback is invoked inline.
    conn->read(
        [fn{std::move(fn)}, mutex, bomb{armBomb()}](
            const tensorpipe::Error& error, const void* ptr, size_t len) {
          std::lock_guard<std::mutex> innerLock(*mutex);
          bomb->defuse();
          fn(error, ptr, len);
        });
  }

  void doRead(
      std::shared_ptr<tensorpipe::transport::Connection> conn,
      void* ptr,
      size_t length,
      tensorpipe::transport::Connection::read_callback_fn fn) {
    auto mutex = std::make_shared<std::mutex>();
    std::lock_guard<std::mutex> outerLock(*mutex);
    // We acquire the same mutex while calling read and inside its callback so
    // that we deadlock if the callback is invoked inline.
    conn->read(
        ptr,
        length,
        [fn{std::move(fn)}, mutex, bomb{armBomb()}](
            const tensorpipe::Error& error, const void* ptr, size_t len) {
          std::lock_guard<std::mutex> innerLock(*mutex);
          bomb->defuse();
          fn(error, ptr, len);
        });
  }

  void doWrite(
      std::shared_ptr<tensorpipe::transport::Connection> conn,
      const void* ptr,
      size_t length,
      tensorpipe::transport::Connection::write_callback_fn fn) {
    auto mutex = std::make_shared<std::mutex>();
    // We acquire the same mutex while calling write and inside its callback so
    // that we deadlock if the callback is invoked inline.
    std::lock_guard<std::mutex> outerLock(*mutex);
    conn->write(
        ptr,
        length,
        [fn{std::move(fn)}, mutex, bomb{armBomb()}](
            const tensorpipe::Error& error) {
          std::lock_guard<std::mutex> innerLock(*mutex);
          bomb->defuse();
          fn(error);
        });
  }
};
