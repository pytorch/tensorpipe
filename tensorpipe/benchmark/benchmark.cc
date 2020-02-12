/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/options.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/context.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/context.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <algorithm>
#include <ctime>

using tensorpipe::Error;
using namespace tensorpipe::benchmark;
using namespace tensorpipe::transport;

std::unique_ptr<uint8_t[]> data;
size_t dataLen;
std::vector<uint64_t> latencyMeasures;

std::promise<void> doneProm;
std::future<void> doneFut = doneProm.get_future();

static uint64_t nowInUsec() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (tv.tv_usec) + (tv.tv_sec * 1000000);
}

static uint64_t percentile(const std::vector<uint64_t>& measures, int p) {
  TP_DCHECK_LE(p, 100);
  return measures[(size_t)(p / 100.0 * measures.size())];
}

static void printMeasures(std::vector<uint64_t>& measures) {
  std::sort(measures.begin(), measures.end());
  uint64_t latency = 0;
  for (uint64_t usec : latencyMeasures) {
    latency += usec;
  }
  fprintf(
      stderr,
      "%-15s %-15s %-15s %-12s %-7s %-7s %-7s %-7s\n",
      "# ping-pong",
      "chunk-size",
      "latency (usec)",
      "avg (usec)",
      "p50",
      "p75",
      "p90",
      "p95");
  fprintf(
      stderr,
      "%-15lu %-15lu %-15lu %-12.3f %-7lu %-7lu %-7lu %-7lu\n",
      latencyMeasures.size(),
      dataLen,
      latency,
      (float)latency / latencyMeasures.size(),
      percentile(measures, 50),
      percentile(measures, 75),
      percentile(measures, 90),
      percentile(measures, 95));
}

static std::unique_ptr<uint8_t[]> createData(const int chunkBytes) {
  uint64_t start = nowInUsec();
  auto data = std::unique_ptr<uint8_t[]>(
      new uint8_t[chunkBytes], std::default_delete<uint8_t[]>());
  // Generate fixed data for validation between peers
  for (int i = 0; i < chunkBytes; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  uint64_t end = nowInUsec();
  std::cout << "Generate data: " << end - start << " usec\n";
  return data;
}

static void serverPongPingNonBlock(
    std::shared_ptr<Connection>& conn,
    int& ioNum) {
  conn->read([&](const Error& error, const void* ptr, size_t len) {
    TP_THROW_ASSERT_IF(error) << error.what();
    // Check correctness
    TP_DCHECK(len == dataLen);
    int cmp = memcmp(ptr, data.get(), len);
    TP_DCHECK(cmp == 0);
    // Send ping
    conn->write(data.get(), dataLen, [&](const Error& error) {
      TP_THROW_ASSERT_IF(error) << error.what();

      // Check if ping-pong has finished
      if (--ioNum > 0) {
        serverPongPingNonBlock(conn, ioNum);
      } else {
        // Pong-ping finished - set promise
        doneProm.set_value();
      }
    });
  });
}

// Start with receiving ping
static void runServer(const Options& options) {
  std::string addr = options.address;
  int ioNum = options.io_num;

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
  std::future<std::shared_ptr<Connection>> connFut = connProm.get_future();
  std::shared_ptr<Listener> listener = context->listen(addr);
  listener->accept(
      [&](const Error& error, std::shared_ptr<Connection> connection) {
        TP_THROW_ASSERT_IF(error) << error.what();
        // Save connection and notify conn future obj
        connProm.set_value(connection);
      });
  std::shared_ptr<Connection> conn = connFut.get();

  serverPongPingNonBlock(conn, ioNum);

  doneFut.get();
  conn.reset();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Connection>& conn,
    int& ioNum) {
  uint64_t start = nowInUsec();
  conn->write(data.get(), dataLen, [&, start](const Error& error) {
    TP_THROW_ASSERT_IF(error) << error.what();
    conn->read([&, start](const Error& error, const void* ptr, size_t len) {
      TP_THROW_ASSERT_IF(error) << error.what();
      uint64_t end = nowInUsec();

      // Check correctness
      TP_DCHECK(len == dataLen);
      int cmp = memcmp(ptr, data.get(), len);
      TP_DCHECK(cmp == 0);

      // Client does measurement upon recv
      latencyMeasures.push_back(end - start);
      // Check if ping-pong has finished
      if (--ioNum > 0) {
        clientPingPongNonBlock(conn, ioNum);
      } else {
        // Ping-pong finished - dump benchmark results and set promise
        printMeasures(latencyMeasures);
        doneProm.set_value();
      }
    });
  });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int ioNum = options.io_num;

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

  clientPingPongNonBlock(conn, ioNum);

  doneFut.get();
  conn.reset();
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
  latencyMeasures.reserve(x.io_num);

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
