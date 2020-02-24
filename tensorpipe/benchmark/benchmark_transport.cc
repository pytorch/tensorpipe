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
#include <tensorpipe/transport/shm/context.h>
#include <tensorpipe/transport/uv/context.h>

using namespace tensorpipe;
using namespace tensorpipe::benchmark;
using namespace tensorpipe::transport;

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
    std::shared_ptr<Connection> conn,
    int& ioNum,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  conn->read(
      data.temporary.get(),
      data.len,
      [conn, &ioNum, &doneProm, &data, &measurements](
          const Error& error, const void* ptr, size_t len) {
        TP_THROW_ASSERT_IF(error) << error.what();
        TP_DCHECK_EQ(len, data.len);
        int cmp = memcmp(ptr, data.expected.get(), len);
        TP_DCHECK_EQ(cmp, 0);
        conn->write(
            data.temporary.get(),
            data.len,
            [conn, &ioNum, &doneProm, &data, &measurements](
                const Error& error) {
              TP_THROW_ASSERT_IF(error) << error.what();
              if (--ioNum > 0) {
                serverPongPingNonBlock(
                    conn, ioNum, doneProm, data, measurements);
              } else {
                doneProm.set_value();
              }
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

  std::shared_ptr<Context> context;
  if (options.transport == "shm") {
    context = std::make_shared<shm::Context>();
  } else if (options.transport == "uv") {
    context = std::make_shared<uv::Context>();
  } else {
    // Should never be here
    abort();
  }

  std::promise<std::shared_ptr<Connection>> connProm;
  std::shared_ptr<Listener> listener = context->listen(addr);
  listener->accept([&](const Error& error, std::shared_ptr<Connection> conn) {
    TP_THROW_ASSERT_IF(error) << error.what();
    connProm.set_value(std::move(conn));
  });
  std::shared_ptr<Connection> conn = connProm.get_future().get();

  std::promise<void> doneProm;
  serverPongPingNonBlock(std::move(conn), ioNum, doneProm, data, measurements);

  doneProm.get_future().get();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Connection> conn,
    int& ioNum,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  measurements.markStart();
  conn->write(
      data.expected.get(),
      data.len,
      [conn, &ioNum, &doneProm, &data, &measurements](const Error& error) {
        TP_THROW_ASSERT_IF(error) << error.what();
        conn->read(
            data.temporary.get(),
            data.len,
            [conn, &ioNum, &doneProm, &data, &measurements](
                const Error& error, const void* ptr, size_t len) {
              measurements.markStop();
              TP_THROW_ASSERT_IF(error) << error.what();
              TP_DCHECK_EQ(len, data.len);
              int cmp = memcmp(ptr, data.expected.get(), len);
              TP_DCHECK_EQ(cmp, 0);
              if (--ioNum > 0) {
                clientPingPongNonBlock(
                    conn, ioNum, doneProm, data, measurements);
              } else {
                printMeasurements(measurements, data.len);
                doneProm.set_value();
              }
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

  std::shared_ptr<Context> context;
  if (options.transport == "shm") {
    context = std::make_shared<shm::Context>();
  } else if (options.transport == "uv") {
    context = std::make_shared<uv::Context>();
  } else {
    // Should never be here
    abort();
  }
  std::shared_ptr<Connection> conn = context->connect(addr);

  std::promise<void> doneProm;
  clientPingPongNonBlock(std::move(conn), ioNum, doneProm, data, measurements);

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
