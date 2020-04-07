/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/tensorpipe.h>

#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <string>

#include <gtest/gtest.h>

using namespace tensorpipe;

namespace {

::testing::AssertionResult buffersAreEqual(
    const void* ptr1,
    const size_t len1,
    const void* ptr2,
    const size_t len2) {
  if (ptr1 == nullptr && ptr2 == nullptr) {
    if (len1 == 0 && len2 == 0) {
      return ::testing::AssertionSuccess();
    }
    if (len1 != 0) {
      return ::testing::AssertionFailure()
          << "first pointer is null but length isn't 0";
    }
    if (len1 != 0) {
      return ::testing::AssertionFailure()
          << "second pointer is null but length isn't 0";
    }
  }
  if (ptr1 == nullptr) {
    return ::testing::AssertionFailure()
        << "first pointer is null but second one isn't";
  }
  if (ptr2 == nullptr) {
    return ::testing::AssertionFailure()
        << "second pointer is null but first one isn't";
  }
  if (len1 != len2) {
    return ::testing::AssertionFailure()
        << "first length is " << len1 << " but second one is " << len2;
  }
  if (std::memcmp(ptr1, ptr2, len1) != 0) {
    return ::testing::AssertionFailure() << "buffer contents aren't equal";
  }
  return ::testing::AssertionSuccess();
}

::testing::AssertionResult messagesAreEqual(
    const Message& m1,
    const Message& m2) {
  EXPECT_TRUE(buffersAreEqual(m1.data, m1.length, m2.data, m2.length));
  if (m1.tensors.size() != m2.tensors.size()) {
    return ::testing::AssertionFailure()
        << "first message has " << m1.tensors.size() << " but second has "
        << m2.tensors.size();
  }
  for (size_t idx = 0; idx < m1.tensors.size(); idx++) {
    EXPECT_TRUE(buffersAreEqual(
        m1.tensors[idx].data,
        m1.tensors[idx].length,
        m2.tensors[idx].data,
        m2.tensors[idx].length));
  }
  return ::testing::AssertionSuccess();
}

std::string kMessageData = "I'm a message";
std::string kTensorData = "And I'm a tensor";

Message makeMessage(int numTensors) {
  Message message;
  message.data =
      reinterpret_cast<void*>(const_cast<char*>(kMessageData.data()));
  message.length = kMessageData.length();
  for (int i = 0; i < numTensors; i++) {
    Message::Tensor tensor;
    tensor.data =
        reinterpret_cast<void*>(const_cast<char*>(kTensorData.data()));
    tensor.length = kTensorData.length();
    message.tensors.push_back(std::move(tensor));
  }
  return message;
}

std::string createUniqueShmAddr() {
  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::ostringstream ss;
  // Once we upgrade googletest, also use test_info->test_suite_name() here.
  ss << "shm://tensorpipe_test_" << test_info->name() << "_" << getpid();
  return ss.str();
}

std::vector<std::string> genUrls() {
  std::vector<std::string> res;

#ifdef TP_ENABLE_SHM
  res.push_back(createUniqueShmAddr());
#endif // TP_ENABLE_SHM
  res.push_back("uv://127.0.0.1");

  return res;
}

} // namespace

TEST(Context, ClientPingSerial) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<std::shared_ptr<Pipe>> serverPipePromise;
  std::promise<Message> writtenMessagePromise;
  std::promise<Message> readDescriptorPromise;
  std::promise<Message> readMessagePromise;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
#ifdef TP_ENABLE_SHM
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
#endif // TP_ENABLE_SHM
  context->registerChannel(
      0, "basic", std::make_shared<channel::basic::Context>());
#ifdef TP_ENABLE_CMA
  context->registerChannel(
      -1, "cma", std::make_shared<channel::cma::Context>());
#endif // TP_ENABLE_CMA

  auto listener = context->listen(genUrls());

  auto clientPipe = context->connect(listener->url("uv"));

  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      serverPipePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      serverPipePromise.set_value(std::move(pipe));
    }
  });
  std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

  clientPipe->write(makeMessage(1), [&](const Error& error, Message message) {
    if (error) {
      writtenMessagePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      writtenMessagePromise.set_value(std::move(message));
    }
  });

  serverPipe->readDescriptor([&](const Error& error, Message message) {
    if (error) {
      readDescriptorPromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      readDescriptorPromise.set_value(std::move(message));
    }
  });

  Message message(readDescriptorPromise.get_future().get());
  auto messageData = std::make_unique<uint8_t[]>(message.length);
  message.data = messageData.get();
  buffers.push_back(std::move(messageData));
  for (auto& tensor : message.tensors) {
    auto tensorData = std::make_unique<uint8_t[]>(tensor.length);
    tensor.data = tensorData.get();
    buffers.push_back(std::move(tensorData));
  }

  serverPipe->read(
      std::move(message), [&](const Error& error, Message message) {
        if (error) {
          readMessagePromise.set_exception(
              std::make_exception_ptr(std::runtime_error(error.what())));
        } else {
          readMessagePromise.set_value(std::move(message));
        }
      });

  EXPECT_TRUE(
      messagesAreEqual(readMessagePromise.get_future().get(), makeMessage(1)));
  EXPECT_TRUE(messagesAreEqual(
      writtenMessagePromise.get_future().get(), makeMessage(1)));

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}

TEST(Context, ClientPingInline) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
#ifdef TP_ENABLE_SHM
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
#endif // TP_ENABLE_SHM
  context->registerChannel(
      0, "basic", std::make_shared<channel::basic::Context>());
#ifdef TP_ENABLE_CMA
  context->registerChannel(
      -1, "cma", std::make_shared<channel::cma::Context>());
#endif // TP_ENABLE_CMA

  auto listener = context->listen(genUrls());

  std::shared_ptr<Pipe> serverPipe;
  listener->accept([&serverPipe, &readCompletedProm, &buffers](
                       const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      readCompletedProm.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    serverPipe->readDescriptor([&serverPipe, &readCompletedProm, &buffers](
                                   const Error& error, Message message) {
      if (error) {
        ADD_FAILURE() << error.what();
        readCompletedProm.set_value();
        return;
      }
      auto messageData = std::make_unique<uint8_t[]>(message.length);
      message.data = messageData.get();
      buffers.push_back(std::move(messageData));
      for (auto& tensor : message.tensors) {
        auto tensorData = std::make_unique<uint8_t[]>(tensor.length);
        tensor.data = tensorData.get();
        buffers.push_back(std::move(tensorData));
      }
      serverPipe->read(
          std::move(message),
          [&readCompletedProm, &buffers](
              const Error& error, Message message) mutable {
            if (error) {
              ADD_FAILURE() << error.what();
              readCompletedProm.set_value();
              return;
            }
            EXPECT_TRUE(messagesAreEqual(message, makeMessage(1)));
            readCompletedProm.set_value();
          });
    });
  });

  auto clientPipe = context->connect(listener->url("uv"));
  clientPipe->write(makeMessage(1), [&](const Error& error, Message message) {
    if (error) {
      ADD_FAILURE() << error.what();
      writeCompletedProm.set_value();
      return;
    }
    writeCompletedProm.set_value();
  });

  readCompletedProm.get_future().get();
  writeCompletedProm.get_future().get();

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}

TEST(Context, ServerPingPongTwice) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<void> pingCompletedProm;
  std::promise<void> pongCompletedProm;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
#ifdef TP_ENABLE_SHM
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
#endif // TP_ENABLE_SHM
  context->registerChannel(
      0, "basic", std::make_shared<channel::basic::Context>());
#ifdef TP_ENABLE_CMA
  context->registerChannel(
      -1, "cma", std::make_shared<channel::cma::Context>());
#endif // TP_ENABLE_CMA

  auto listener = context->listen(genUrls());

  std::shared_ptr<Pipe> serverPipe;
  int numPingsGoneThrough = 0;
  listener->accept([&serverPipe,
                    &pingCompletedProm,
                    &buffers,
                    &numPingsGoneThrough](
                       const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      pingCompletedProm.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    for (int i = 0; i < 2; i++) {
      serverPipe->write(
          makeMessage(1),
          [&serverPipe, &pingCompletedProm, &buffers, &numPingsGoneThrough, i](
              const Error& error, Message message) {
            if (error) {
              ADD_FAILURE() << error.what();
              pingCompletedProm.set_value();
              return;
            }
            serverPipe->readDescriptor([&serverPipe,
                                        &pingCompletedProm,
                                        &buffers,
                                        &numPingsGoneThrough,
                                        i](const Error& error,
                                           Message message) {
              if (error) {
                ADD_FAILURE() << error.what();
                pingCompletedProm.set_value();
                return;
              }
              auto messageData = std::make_unique<uint8_t[]>(message.length);
              message.data = messageData.get();
              buffers.push_back(std::move(messageData));
              for (auto& tensor : message.tensors) {
                auto tensorData = std::make_unique<uint8_t[]>(tensor.length);
                tensor.data = tensorData.get();
                buffers.push_back(std::move(tensorData));
              }
              serverPipe->read(
                  std::move(message),
                  [&pingCompletedProm, &buffers, &numPingsGoneThrough, i](
                      const Error& error, Message message) {
                    if (error) {
                      ADD_FAILURE() << error.what();
                      pingCompletedProm.set_value();
                      return;
                    }
                    EXPECT_TRUE(messagesAreEqual(message, makeMessage(1)));
                    EXPECT_EQ(numPingsGoneThrough, i);
                    numPingsGoneThrough++;
                    if (numPingsGoneThrough == 2) {
                      pingCompletedProm.set_value();
                    }
                  });
            });
          });
    }
  });

  auto clientPipe = context->connect(listener->url("uv"));
  int numPongsGoneThrough = 0;
  for (int i = 0; i < 2; i++) {
    clientPipe->readDescriptor([&clientPipe,
                                &pongCompletedProm,
                                &buffers,
                                &numPongsGoneThrough,
                                i](const Error& error, Message message) {
      if (error) {
        ADD_FAILURE() << error.what();
        pongCompletedProm.set_value();
        return;
      }
      auto messageData = std::make_unique<uint8_t[]>(message.length);
      message.data = messageData.get();
      buffers.push_back(std::move(messageData));
      for (auto& tensor : message.tensors) {
        auto tensorData = std::make_unique<uint8_t[]>(tensor.length);
        tensor.data = tensorData.get();
        buffers.push_back(std::move(tensorData));
      }
      clientPipe->read(
          std::move(message),
          [&clientPipe, &pongCompletedProm, &buffers, &numPongsGoneThrough, i](
              const Error& error, Message message) mutable {
            if (error) {
              ADD_FAILURE() << error.what();
              pongCompletedProm.set_value();
              return;
            }
            clientPipe->write(
                std::move(message),
                [&pongCompletedProm, &buffers, &numPongsGoneThrough, i](
                    const Error& error, Message message) {
                  if (error) {
                    ADD_FAILURE() << error.what();
                    pongCompletedProm.set_value();
                    return;
                  }
                  EXPECT_EQ(numPongsGoneThrough, i);
                  numPongsGoneThrough++;
                  if (numPongsGoneThrough == 2) {
                    pongCompletedProm.set_value();
                  }
                });
          });
    });
  }

  pingCompletedProm.get_future().get();
  pongCompletedProm.get_future().get();

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}

static void pipeRead(
    std::shared_ptr<Pipe>& pipe,
    std::vector<std::unique_ptr<uint8_t[]>>& buffers,
    std::function<void(const Error&, Message)> fn) {
  pipe->readDescriptor([&pipe, &buffers, fn{std::move(fn)}](
                           const Error& error, Message message) mutable {
    ASSERT_FALSE(error);
    auto messageData = std::make_unique<uint8_t[]>(message.length);
    message.data = messageData.get();
    buffers.push_back(std::move(messageData));
    for (auto& tensor : message.tensors) {
      auto tensorData = std::make_unique<uint8_t[]>(tensor.length);
      tensor.data = tensorData.get();
      buffers.push_back(std::move(tensorData));
    }
    pipe->read(
        std::move(message),
        [fn{std::move(fn)}](const Error& error, Message message) mutable {
          fn(error, std::move(message));
        });
  });
}

TEST(Context, MixedTensorMessage) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<void> writeCompletedProm;
  std::promise<void> readCompletedProm;
  int n = 2;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
#ifdef TP_ENABLE_SHM
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
#endif // TP_ENABLE_SHM
  context->registerChannel(
      0, "basic", std::make_shared<channel::basic::Context>());
#ifdef TP_ENABLE_CMA
  context->registerChannel(
      -1, "cma", std::make_shared<channel::cma::Context>());
#endif // TP_ENABLE_CMA

  auto listener = context->listen(genUrls());

  std::shared_ptr<Pipe> serverPipe;
  std::atomic<int> readNum(n);
  listener->accept([&serverPipe, &readCompletedProm, &buffers, &readNum](
                       const Error& error, std::shared_ptr<Pipe> pipe) {
    ASSERT_FALSE(error);
    serverPipe = std::move(pipe);
    pipeRead(serverPipe, buffers, [&](const Error& error, Message message) {
      ASSERT_FALSE(error);
      EXPECT_TRUE(messagesAreEqual(message, makeMessage(1)));
      if (--readNum == 0) {
        readCompletedProm.set_value();
      }
    });
    pipeRead(serverPipe, buffers, [&](const Error& error, Message message) {
      ASSERT_FALSE(error);
      EXPECT_TRUE(messagesAreEqual(message, makeMessage(0)));
      if (--readNum == 0) {
        readCompletedProm.set_value();
      }
    });
  });

  auto clientPipe = context->connect(listener->url("uv"));
  std::atomic<int> writeNum(n);
  clientPipe->write(makeMessage(1), [&](const Error& error, Message message) {
    ASSERT_FALSE(error);
    if (--writeNum == 0) {
      writeCompletedProm.set_value();
    }
  });
  clientPipe->write(makeMessage(0), [&](const Error& error, Message message) {
    ASSERT_FALSE(error);
    if (--writeNum == 0) {
      writeCompletedProm.set_value();
    }
  });

  readCompletedProm.get_future().get();
  writeCompletedProm.get_future().get();

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}
