/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/basic.h>
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

std::unique_ptr<uint8_t[]> copyStringToBuffer(const std::string& s) {
  auto b = std::make_unique<uint8_t[]>(s.size());
  std::memcpy(b.get(), s.data(), s.size());
  return b;
}

::testing::AssertionResult buffersAreEqual(
    const std::unique_ptr<uint8_t[]>& ptr1,
    const size_t len1,
    const std::unique_ptr<uint8_t[]>& ptr2,
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
  if (std::memcmp(ptr1.get(), ptr2.get(), len1) != 0) {
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

Message makeMessage() {
  Message message;
  std::string messageData = "I'm a message";
  std::string tensorData = "And I'm a tensor";
  message.data = copyStringToBuffer(messageData);
  message.length = messageData.length();
  Message::Tensor tensor;
  tensor.data = copyStringToBuffer(tensorData);
  tensor.length = tensorData.length();
  message.tensors.push_back(std::move(tensor));
  return message;
}

TEST(Context, ClientPingSerial) {
  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());

  auto listener = Listener::create(context, {"shm://foobar", "uv://127.0.0.1"});

  auto clientPipe = Pipe::create(context, listener->url("uv"));

  std::promise<std::shared_ptr<Pipe>> serverPipePromise;
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      serverPipePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      serverPipePromise.set_value(std::move(pipe));
    }
  });
  std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

  std::promise<Message> writtenMessagePromise;
  clientPipe->write(makeMessage(), [&](const Error& error, Message&& message) {
    if (error) {
      writtenMessagePromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      writtenMessagePromise.set_value(std::move(message));
    }
  });

  std::promise<Message> readDescriptorPromise;
  serverPipe->readDescriptor([&](const Error& error, Message&& message) {
    if (error) {
      readDescriptorPromise.set_exception(
          std::make_exception_ptr(std::runtime_error(error.what())));
    } else {
      readDescriptorPromise.set_value(std::move(message));
    }
  });

  Message message(readDescriptorPromise.get_future().get());
  message.data = std::make_unique<uint8_t[]>(message.length);
  for (auto& tensor : message.tensors) {
    tensor.data = std::make_unique<uint8_t[]>(tensor.length);
  }

  std::promise<Message> readMessagePromise;
  serverPipe->read(
      std::move(message), [&](const Error& error, Message&& message) {
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
  std::promise<void> donePromise;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());

  auto listener = Listener::create(context, {"shm://foobar", "uv://127.0.0.1"});

  std::shared_ptr<Pipe> serverPipe;
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      donePromise.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    serverPipe->readDescriptor([&](const Error& error, Message&& message) {
      if (error) {
        ADD_FAILURE() << error.what();
        donePromise.set_value();
        return;
      }
      message.data = std::make_unique<uint8_t[]>(message.length);
      for (auto& tensor : message.tensors) {
        tensor.data = std::make_unique<uint8_t[]>(tensor.length);
      }
      serverPipe->read(
          std::move(message), [&](const Error& error, Message&& message) {
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
  clientPipe->write(makeMessage(), [&](const Error& error, Message&& message) {
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
  std::promise<void> donePromise;

  auto context = Context::create();

  context->registerTransport(
      0, "uv", std::make_shared<transport::uv::Context>());
  context->registerTransport(
      -1, "shm", std::make_shared<transport::shm::Context>());
  context->registerChannelFactory(
      0, "basic", std::make_shared<channel::basic::BasicChannelFactory>());

  auto listener = Listener::create(context, {"shm://foobar", "uv://127.0.0.1"});

  std::shared_ptr<Pipe> serverPipe;

  int numPingsGoneThrough = 0;
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    if (error) {
      ADD_FAILURE() << error.what();
      donePromise.set_value();
      return;
    }
    serverPipe = std::move(pipe);
    for (int i = 0; i < 2; i++) {
      serverPipe->write(
          makeMessage(),
          [serverPipe, &donePromise, &numPingsGoneThrough, i](
              const Error& error, Message&& message) {
            if (error) {
              ADD_FAILURE() << error.what();
              donePromise.set_value();
              return;
            }
            serverPipe->readDescriptor(
                [serverPipe, &donePromise, &numPingsGoneThrough, i](
                    const Error& error, Message&& message) {
                  if (error) {
                    ADD_FAILURE() << error.what();
                    donePromise.set_value();
                    return;
                  }
                  message.data = std::make_unique<uint8_t[]>(message.length);
                  for (auto& tensor : message.tensors) {
                    tensor.data = std::make_unique<uint8_t[]>(tensor.length);
                  }
                  serverPipe->read(
                      std::move(message),
                      [&donePromise, &numPingsGoneThrough, i](
                          const Error& error, Message&& message) {
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
    clientPipe->readDescriptor([&](const Error& error, Message&& message) {
      if (error) {
        ADD_FAILURE() << error.what();
        donePromise.set_value();
        return;
      }
      message.data = std::make_unique<uint8_t[]>(message.length);
      for (auto& tensor : message.tensors) {
        tensor.data = std::make_unique<uint8_t[]>(tensor.length);
      }
      clientPipe->read(
          std::move(message), [&](const Error& error, Message&& message) {
            if (error) {
              ADD_FAILURE() << error.what();
              donePromise.set_value();
              return;
            }
            clientPipe->write(
                std::move(message), [&](const Error& error, Message&& message) {
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
