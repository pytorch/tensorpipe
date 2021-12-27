/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>

#include <future>

#include <tensorpipe/benchmark/measurements.h>
#include <tensorpipe/benchmark/options.h>
#include <tensorpipe/benchmark/transport_registry.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/listener.h>

using namespace tensorpipe;
using namespace tensorpipe::benchmark;
using namespace tensorpipe::transport;

struct Data {
  std::unique_ptr<uint8_t[]> expected;
  std::unique_ptr<uint8_t[]> temporary;
  size_t size;
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

static std::unique_ptr<uint8_t[]> createData(const int size) {
  auto data = std::make_unique<uint8_t[]>(size);
  // Generate fixed data for validation between peers
  for (int i = 0; i < size; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  return data;
}

static void serverPongPingNonBlock(
    std::shared_ptr<Connection> conn,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  conn->read(
      data.temporary.get(),
      data.size,
      [conn, &numRoundTrips, &doneProm, &data, &measurements](
          const Error& error, const void* ptr, size_t len) {
        TP_THROW_ASSERT_IF(error) << error.what();
        TP_DCHECK_EQ(len, data.size);
        TP_DCHECK_EQ(memcmp(ptr, data.expected.get(), len), 0);
        conn->write(
            data.temporary.get(),
            data.size,
            [conn, &numRoundTrips, &doneProm, &data, &measurements](
                const Error& error) {
              TP_THROW_ASSERT_IF(error) << error.what();
              if (--numRoundTrips > 0) {
                serverPongPingNonBlock(
                    conn, numRoundTrips, doneProm, data, measurements);
              } else {
                doneProm.set_value();
              }
            });
      });
}

// Start with receiving ping
static void runServer(const Options& options) {
  std::string addr = options.address;
  int numRoundTrips = options.numRoundTrips;
  Data data = {
      createData(options.payloadSize),
      std::make_unique<uint8_t[]>(options.payloadSize),
      options.payloadSize};
  Measurements measurements;
  measurements.reserve(options.numRoundTrips);

  std::shared_ptr<transport::Context> context;
  context = TensorpipeTransportRegistry().create(options.transport);
  validateTransportContext(context);

  std::promise<std::shared_ptr<Connection>> connProm;
  std::shared_ptr<transport::Listener> listener = context->listen(addr);
  listener->accept([&](const Error& error, std::shared_ptr<Connection> conn) {
    TP_THROW_ASSERT_IF(error) << error.what();
    connProm.set_value(std::move(conn));
  });
  std::shared_ptr<Connection> conn = connProm.get_future().get();

  std::promise<void> doneProm;
  serverPongPingNonBlock(
      std::move(conn), numRoundTrips, doneProm, data, measurements);

  doneProm.get_future().get();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Connection> conn,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  measurements.markStart();
  conn->write(
      data.expected.get(),
      data.size,
      [conn, &numRoundTrips, &doneProm, &data, &measurements](
          const Error& error) {
        TP_THROW_ASSERT_IF(error) << error.what();
        conn->read(
            data.temporary.get(),
            data.size,
            [conn, &numRoundTrips, &doneProm, &data, &measurements](
                const Error& error, const void* ptr, size_t len) {
              measurements.markStop();
              TP_THROW_ASSERT_IF(error) << error.what();
              TP_DCHECK_EQ(len, data.size);
              TP_DCHECK_EQ(memcmp(ptr, data.expected.get(), len), 0);
              if (--numRoundTrips > 0) {
                clientPingPongNonBlock(
                    conn, numRoundTrips, doneProm, data, measurements);
              } else {
                printMeasurements(measurements, data.size);
                doneProm.set_value();
              }
            });
      });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int numRoundTrips = options.numRoundTrips;
  Data data = {
      createData(options.payloadSize),
      std::make_unique<uint8_t[]>(options.payloadSize),
      options.payloadSize};
  Measurements measurements;
  measurements.reserve(options.numRoundTrips);

  std::shared_ptr<transport::Context> context;
  context = TensorpipeTransportRegistry().create(options.transport);
  validateTransportContext(context);
  std::shared_ptr<Connection> conn = context->connect(addr);

  std::promise<void> doneProm;
  clientPingPongNonBlock(
      std::move(conn), numRoundTrips, doneProm, data, measurements);

  doneProm.get_future().get();
  context->join();
}

int main(int argc, char** argv) {
  struct Options x = parseOptions(argc, argv);
  std::cout << "mode = " << x.mode << "\n";
  std::cout << "transport = " << x.transport << "\n";
  std::cout << "address = " << x.address << "\n";
  std::cout << "num_round_trips = " << x.numRoundTrips << "\n";
  std::cout << "payload_size = " << x.payloadSize << "\n";

  if (x.mode == "listen") {
    runServer(x);
  } else if (x.mode == "connect") {
    runClient(x);
  } else {
    // Should never be here
    TP_THROW_ASSERT() << "unknown mode: " << x.mode;
  }

  return 0;
}
