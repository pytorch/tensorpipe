/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>

#include <condition_variable>
#include <cstring>
#include <mutex>

#include <gtest/gtest.h>

using namespace tensorpipe;

std::unique_ptr<uint8_t[]> copyStringToBuffer(const std::string& s) {
  auto b = std::make_unique<uint8_t[]>(s.size());
  std::memcpy(b.get(), s.data(), s.size());
  return b;
}

bool compareStringToBuffer(
    const std::string& s,
    const std::unique_ptr<uint8_t[]>& ptr,
    size_t len) {
  return ptr && len == s.size() &&
      std::memcmp(ptr.get(), s.data(), s.size()) == 0;
}

class Defer {
 public:
  Defer(std::function<void()> f) : f_(std::move(f)){};
  ~Defer() {
    f_();
  };

 private:
  std::function<void()> f_;
};

TEST(Context, Simple) {
  Message sentMessage;
  std::string messageData = "I'm a message";
  std::string tensorData = "And I'm a tensor";
  sentMessage.data = copyStringToBuffer(messageData);
  sentMessage.length = messageData.length();
  Message::Tensor sentTensor;
  sentTensor.data = copyStringToBuffer(tensorData);
  sentTensor.length = tensorData.length();
  sentMessage.tensors.push_back(std::move(sentTensor));

  bool done = false;
  std::mutex mutex;
  std::condition_variable cv;

  std::vector<std::string> transports;
  transports.push_back("shm");
  transports.push_back("uv");
  auto context = Context::create(std::move(transports));

  std::vector<std::string> addresses;
  addresses.push_back("shm://foobar");
  addresses.push_back("uv://127.0.0.1");
  auto listener = Listener::create(context, std::move(addresses));

  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      std::unique_lock<std::mutex> lock(mutex);
      Defer d([&]() {
        done = true;
        cv.notify_all();
      });
      ASSERT_TRUE(false) << error.what();
    }
    pipe->readDescriptor([&, pipe](const Error& error, Message&& message) {
      if (error) {
        std::unique_lock<std::mutex> lock(mutex);
        Defer d([&]() {
          done = true;
          cv.notify_all();
        });
        ASSERT_TRUE(false) << error.what();
      }
      message.data = std::make_unique<uint8_t[]>(message.length);
      for (auto& tensor : message.tensors) {
        tensor.data = std::make_unique<uint8_t[]>(tensor.length);
      }
      pipe->read(
          std::move(message), [&, pipe](const Error& error, Message&& message) {
            if (error) {
              std::unique_lock<std::mutex> lock(mutex);
              Defer d([&]() {
                done = true;
                cv.notify_all();
              });
              ASSERT_TRUE(false) << error.what();
            }
            std::unique_lock<std::mutex> lock(mutex);
            Defer d([&]() {
              done = true;
              cv.notify_all();
            });
            ASSERT_TRUE(compareStringToBuffer(
                messageData, message.data, message.length));
            ASSERT_EQ(message.tensors.size(), 1);
            ASSERT_TRUE(compareStringToBuffer(
                tensorData,
                message.tensors[0].data,
                message.tensors[0].length));
          });
    });
  });

  auto clientPipe = Pipe::create(context, listener->url("uv"));
  clientPipe->write(
      std::move(sentMessage), [&](const Error& error, Message&& message) {
        if (error) {
          std::unique_lock<std::mutex> lock(mutex);
          EXPECT_TRUE(false) << error.what();
          cv.notify_all();
        }
      });

  std::unique_lock<std::mutex> lock(mutex);
  while (!done) {
    cv.wait(lock);
  }

  listener.reset();
  clientPipe.reset();
  context->join();
}
