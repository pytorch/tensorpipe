/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/measurements.h>
#include <tensorpipe/benchmark/options.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>
#include <tensorpipe/transport/shm/context.h>
#include <tensorpipe/transport/uv/context.h>

using namespace tensorpipe;
using namespace tensorpipe::benchmark;

std::unique_ptr<uint8_t[]> data;
size_t dataLen;

std::promise<void> doneProm;
std::future<void> doneFut = doneProm.get_future();

Measurements measurements;

static void printMeasurements() {
  measurements.sort();
  fprintf(
      stderr,
      "%-15s %-15s %-12s %-7s %-7s %-7s %-7s\n",
      "chunk-size",
      "# ping-pong",
      "avg (usec)",
      "p50",
      "p75",
      "p90",
      "p95");
  fprintf(
      stderr,
      "%-15lu %-15lu %-12.3f %-7.3f %-7.3f %-7.3f %-7.3f\n",
      dataLen,
      measurements.size(),
      measurements.sum().count() / (float)measurements.size() / 1000.0,
      measurements.percentile(0.50).count() / 1000.0,
      measurements.percentile(0.75).count() / 1000.0,
      measurements.percentile(0.90).count() / 1000.0,
      measurements.percentile(0.95).count() / 1000.0);
}

static std::unique_ptr<uint8_t[]> createData(const int chunkBytes) {
  auto data = std::unique_ptr<uint8_t[]>(
      new uint8_t[chunkBytes], std::default_delete<uint8_t[]>());
  // Generate fixed data for validation between peers
  for (int i = 0; i < chunkBytes; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  return data;
}

static void serverPongPingNonBlock(std::shared_ptr<Pipe> pipe, int& ioNum) {
  pipe->readDescriptor([pipe, &ioNum](const Error& error, Message&& message) {
    TP_THROW_ASSERT_IF(error) << error.what();
    TP_DCHECK_EQ(message.length, dataLen);
    message.data = std::make_unique<uint8_t[]>(message.length);
    pipe->read(
        std::move(message),
        [pipe, &ioNum](const Error& error, Message&& message) {
          TP_THROW_ASSERT_IF(error) << error.what();
          int cmp = memcmp(message.data.get(), data.get(), message.length);
          TP_DCHECK_EQ(cmp, 0);
          pipe->write(
              std::move(message),
              [pipe, &ioNum](const Error& error, Message&& message) {
                TP_THROW_ASSERT_IF(error) << error.what();
                if (--ioNum > 0) {
                  serverPongPingNonBlock(pipe, ioNum);
                } else {
                  doneProm.set_value();
                }
              });
        });
  });
}

// Start with receiving ping
static void runServer(const Options& options) {
  std::string addr = options.address;
  int ioNum = options.io_num;

  std::shared_ptr<Context> context = Context::create();
  if (options.transport == "shm") {
    context->registerTransport(
        0, "shm", std::make_shared<transport::shm::Context>());
  } else if (options.transport == "uv") {
    context->registerTransport(
        0, "uv", std::make_shared<transport::uv::Context>());
  } else {
    // Should never be here
    abort();
  }

  std::promise<std::shared_ptr<Pipe>> pipeProm;
  std::future<std::shared_ptr<Pipe>> pipeFut = pipeProm.get_future();
  std::shared_ptr<Listener> listener = Listener::create(context, {addr});
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    TP_THROW_ASSERT_IF(error) << error.what();
    // Save connection and notify conn future obj
    pipeProm.set_value(std::move(pipe));
  });
  std::shared_ptr<Pipe> pipe = pipeFut.get();

  serverPongPingNonBlock(std::move(pipe), ioNum);

  doneFut.get();
  listener.reset();
  context->join();
}

static void clientPingPongNonBlock(std::shared_ptr<Pipe> pipe, int& ioNum) {
  measurements.markStart();
  Message message;
  message.data = std::move(data);
  message.length = dataLen;
  pipe->write(
      std::move(message),
      [pipe, &ioNum](const Error& error, Message&& message) {
        TP_THROW_ASSERT_IF(error) << error.what();
        data = std::move(message.data);
        pipe->readDescriptor(
            [pipe, &ioNum](const Error& error, Message&& message) {
              TP_THROW_ASSERT_IF(error) << error.what();
              TP_DCHECK_EQ(message.length, dataLen);
              message.data = std::make_unique<uint8_t[]>(message.length);
              pipe->read(
                  std::move(message),
                  [pipe, &ioNum](const Error& error, Message&& message) {
                    TP_THROW_ASSERT_IF(error) << error.what();
                    measurements.markStop();
                    int cmp =
                        memcmp(message.data.get(), data.get(), message.length);
                    TP_DCHECK_EQ(cmp, 0);
                    if (--ioNum > 0) {
                      clientPingPongNonBlock(pipe, ioNum);
                    } else {
                      printMeasurements();
                      doneProm.set_value();
                    }
                  });
            });
      });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int ioNum = options.io_num;

  std::shared_ptr<Context> context = Context::create();
  if (options.transport == "shm") {
    context->registerTransport(
        0, "shm", std::make_shared<transport::shm::Context>());
  } else if (options.transport == "uv") {
    context->registerTransport(
        0, "uv", std::make_shared<transport::uv::Context>());
  } else {
    // Should never be here
    abort();
  }
  std::shared_ptr<Pipe> pipe = Pipe::create(context, addr);

  clientPingPongNonBlock(std::move(pipe), ioNum);

  doneFut.get();
  context->join();
}

int main(int argc, char** argv) {
  struct Options x = parseOptions(argc, argv);
  std::cout << "mode = " << x.mode << "\n";
  std::cout << "transport = " << x.transport << "\n";
  std::cout << "address = " << x.address << "\n";
  std::cout << "io_num = " << x.io_num << "\n";
  std::cout << "chunk_bytes = " << x.chunk_bytes << "\n";

  // Initialize global
  data = createData(x.chunk_bytes);
  dataLen = x.chunk_bytes;
  measurements.reserve(x.io_num);

  if (x.mode == "listen") {
    runServer(x);
  } else if (x.mode == "connect") {
    runClient(x);
  } else {
    // Should never be here
    abort();
  }

  return 0;
}
