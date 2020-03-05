/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>
#include <tensorpipe/channel/intra_process/intra_process.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/shm/context.h>
#include <tensorpipe/transport/uv/context.h>

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

Message makeMessage() {
  Message message;
  message.data =
      reinterpret_cast<void*>(const_cast<char*>(kMessageData.data()));
  message.length = kMessageData.length();
  Message::Tensor tensor;
  tensor.data = reinterpret_cast<void*>(const_cast<char*>(kTensorData.data()));
  tensor.length = kTensorData.length();
  message.tensors.push_back(std::move(tensor));
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
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());
  context->registerChannelFactory(
      -1,
      "intra_process",
      std::make_shared<
          channel::intra_process::IntraProcessChannelFactory>());

  auto listener =
      Listener::create(context, {createUniqueShmAddr(), "uv://127.0.0.1"});

  auto clientPipe = Pipe::create(context, listener->url("uv"));

  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      serverPipePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      serverPipePromise.set_value(std::move(pipe));
    }
  });
  std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

  clientPipe->write(makeMessage(), [&](const Error& error, Message message) {
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
      messagesAreEqual(readMessagePromise.get_future().get(), makeMessage()));
  EXPECT_TRUE(messagesAreEqual(
      writtenMessagePromise.get_future().get(), makeMessage()));

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}

TEST(Context, ClientPingInline) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<void> donePromise;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());
  context->registerChannelFactory(
      -1,
      "intra_process",
      std::make_shared<
          channel::intra_process::IntraProcessChannelFactory>());

  auto listener =
      Listener::create(context, {createUniqueShmAddr(), "uv://127.0.0.1"});

  std::shared_ptr<Pipe> serverPipe;
  listener->accept([&serverPipe, &donePromise, &buffers](
                       const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      donePromise.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    serverPipe->readDescriptor([&serverPipe, &donePromise, &buffers](
                                   const Error& error, Message message) {
      if (error) {
        ADD_FAILURE() << error.what();
        donePromise.set_value();
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
          [&donePromise, &buffers](
              const Error& error, Message message) mutable {
            if (error) {
              ADD_FAILURE() << error.what();
              donePromise.set_value();
              return;
            }
            EXPECT_TRUE(messagesAreEqual(message, makeMessage()));
            donePromise.set_value();
          });
    });
  });

  auto clientPipe = Pipe::create(context, listener->url("uv"));
  clientPipe->write(makeMessage(), [&](const Error& error, Message message) {
    if (error) {
      ADD_FAILURE() << error.what();
      donePromise.set_value();
      return;
    }
  });

  donePromise.get_future().get();

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}

TEST(Context, ServerPingPongTwice) {
  std::vector<std::unique_ptr<uint8_t[]>> buffers;
  std::promise<void> donePromise;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());
  context->registerChannelFactory(
      -1,
      "intra_process",
      std::make_shared<
          channel::intra_process::IntraProcessChannelFactory>());

  auto listener =
      Listener::create(context, {createUniqueShmAddr(), "uv://127.0.0.1"});

  std::shared_ptr<Pipe> serverPipe;
  int numPingsGoneThrough = 0;
  listener->accept([&serverPipe, &donePromise, &buffers, &numPingsGoneThrough](
                       const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      donePromise.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    for (int i = 0; i < 2; i++) {
      serverPipe->write(
          makeMessage(),
          [&serverPipe, &donePromise, &buffers, &numPingsGoneThrough, i](
              const Error& error, Message message) {
            if (error) {
              ADD_FAILURE() << error.what();
              donePromise.set_value();
              return;
            }
            serverPipe->readDescriptor(
                [&serverPipe, &donePromise, &buffers, &numPingsGoneThrough, i](
                    const Error& error, Message message) {
                  if (error) {
                    ADD_FAILURE() << error.what();
                    donePromise.set_value();
                    return;
                  }
                  auto messageData =
                      std::make_unique<uint8_t[]>(message.length);
                  message.data = messageData.get();
                  buffers.push_back(std::move(messageData));
                  for (auto& tensor : message.tensors) {
                    auto tensorData =
                        std::make_unique<uint8_t[]>(tensor.length);
                    tensor.data = tensorData.get();
                    buffers.push_back(std::move(tensorData));
                  }
                  serverPipe->read(
                      std::move(message),
                      [&donePromise, &buffers, &numPingsGoneThrough, i](
                          const Error& error, Message message) {
                        if (error) {
                          ADD_FAILURE() << error.what();
                          donePromise.set_value();
                          return;
                        }
                        EXPECT_TRUE(messagesAreEqual(message, makeMessage()));
                        EXPECT_EQ(numPingsGoneThrough, i);
                        numPingsGoneThrough++;
                        if (numPingsGoneThrough == 2) {
                          donePromise.set_value();
                        }
                      });
                });
          });
    }
  });

  auto clientPipe = Pipe::create(context, listener->url("uv"));
  for (int i = 0; i < 2; i++) {
    clientPipe->readDescriptor([&clientPipe, &donePromise, &buffers](
                                   const Error& error, Message message) {
      if (error) {
        ADD_FAILURE() << error.what();
        donePromise.set_value();
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
          [&clientPipe, &donePromise, &buffers](
              const Error& error, Message message) mutable {
            if (error) {
              ADD_FAILURE() << error.what();
              donePromise.set_value();
              return;
            }
            clientPipe->write(
                std::move(message),
                [&donePromise, &buffers](const Error& error, Message message) {
                  if (error) {
                    ADD_FAILURE() << error.what();
                    donePromise.set_value();
                    return;
                  }
                });
          });
    });
  }

  donePromise.get_future().get();

  serverPipe.reset();
  listener.reset();
  clientPipe.reset();
  context->join();
}
