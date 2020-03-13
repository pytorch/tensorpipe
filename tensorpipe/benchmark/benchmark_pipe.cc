/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/measurements.h>
#include <tensorpipe/benchmark/options.h>
#include <tensorpipe/tensorpipe.h>

using namespace tensorpipe;
using namespace tensorpipe::benchmark;

Measurements measurements;

struct Data {
  std::unique_ptr<uint8_t[]> expected;
  std::unique_ptr<uint8_t[]> temporary;
  size_t len;
};

static void printMeasurements(Measurements& measurements, size_t dataLen) {
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
  auto data = std::make_unique<uint8_t[]>(chunkBytes);
  // Generate fixed data for validation between peers
  for (int i = 0; i < chunkBytes; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  return data;
}

static void serverPongPingNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& ioNum,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  pipe->readDescriptor([pipe, &ioNum, &doneProm, &data, &measurements](
                           const Error& error, Message&& message) {
    TP_THROW_ASSERT_IF(error) << error.what();
    TP_DCHECK_EQ(message.length, data.len);
    message.data = data.temporary.get();
    pipe->read(
        std::move(message),
        [pipe, &ioNum, &doneProm, &data, &measurements](
            const Error& error, Message&& message) {
          TP_THROW_ASSERT_IF(error) << error.what();
          int cmp = memcmp(message.data, data.expected.get(), message.length);
          TP_DCHECK_EQ(cmp, 0);
          pipe->write(
              std::move(message),
              [pipe, &ioNum, &doneProm, &data, &measurements](
                  const Error& error, Message&& message) {
                TP_THROW_ASSERT_IF(error) << error.what();
                if (--ioNum > 0) {
                  serverPongPingNonBlock(
                      pipe, ioNum, doneProm, data, measurements);
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
  int ioNum = options.ioNum;
  Data data = {createData(options.chunkBytes),
               std::make_unique<uint8_t[]>(options.chunkBytes),
               options.chunkBytes};
  Measurements measurements;
  measurements.reserve(options.ioNum);

  std::shared_ptr<Context> context = Context::create();
#ifdef TP_ENABLE_SHM
  if (options.transport == "shm") {
    context->registerTransport(
        0, "shm", std::make_shared<transport::shm::Context>());
  } else
#endif // TP_ENABLE_SHM
      if (options.transport == "uv") {
    context->registerTransport(
        0, "uv", std::make_shared<transport::uv::Context>());
  } else {
    // Should never be here
    abort();
  }

  std::promise<std::shared_ptr<Pipe>> pipeProm;
  std::shared_ptr<Listener> listener = Listener::create(context, {addr});
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    TP_THROW_ASSERT_IF(error) << error.what();
    pipeProm.set_value(std::move(pipe));
  });
  std::shared_ptr<Pipe> pipe = pipeProm.get_future().get();

  std::promise<void> doneProm;
  serverPongPingNonBlock(std::move(pipe), ioNum, doneProm, data, measurements);

  doneProm.get_future().get();
  listener.reset();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& ioNum,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  measurements.markStart();
  Message message;
  message.data = data.expected.get();
  message.length = data.len;
  pipe->write(
      std::move(message),
      [pipe, &ioNum, &doneProm, &data, &measurements](
          const Error& error, Message&& message) {
        TP_THROW_ASSERT_IF(error) << error.what();
        pipe->readDescriptor([pipe, &ioNum, &doneProm, &data, &measurements](
                                 const Error& error, Message&& message) {
          TP_THROW_ASSERT_IF(error) << error.what();
          TP_DCHECK_EQ(message.length, data.len);
          message.data = data.temporary.get();
          pipe->read(
              std::move(message),
              [pipe, &ioNum, &doneProm, &data, &measurements](
                  const Error& error, Message&& message) {
                measurements.markStop();
                TP_THROW_ASSERT_IF(error) << error.what();
                int cmp =
                    memcmp(message.data, data.expected.get(), message.length);
                TP_DCHECK_EQ(cmp, 0);
                if (--ioNum > 0) {
                  clientPingPongNonBlock(
                      pipe, ioNum, doneProm, data, measurements);
                } else {
                  printMeasurements(measurements, data.len);
                  doneProm.set_value();
                }
              });
        });
      });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int ioNum = options.ioNum;
  Data data = {createData(options.chunkBytes),
               std::make_unique<uint8_t[]>(options.chunkBytes),
               options.chunkBytes};
  Measurements measurements;
  measurements.reserve(options.ioNum);

  std::shared_ptr<Context> context = Context::create();
#ifdef TP_ENABLE_SHM
  if (options.transport == "shm") {
    context->registerTransport(
        0, "shm", std::make_shared<transport::shm::Context>());
  } else
#endif // TP_ENABLE_SHM
      if (options.transport == "uv") {
    context->registerTransport(
        0, "uv", std::make_shared<transport::uv::Context>());
  } else {
    // Should never be here
    abort();
  }
  std::shared_ptr<Pipe> pipe = Pipe::create(context, addr);

  std::promise<void> doneProm;
  clientPingPongNonBlock(std::move(pipe), ioNum, doneProm, data, measurements);

  doneProm.get_future().get();
  context->join();
}

int main(int argc, char** argv) {
  struct Options x = parseOptions(argc, argv);
  std::cout << "mode = " << x.mode << "\n";
  std::cout << "transport = " << x.transport << "\n";
  std::cout << "address = " << x.address << "\n";
  std::cout << "io_num = " << x.ioNum << "\n";
  std::cout << "chunk_bytes = " << x.chunkBytes << "\n";

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
