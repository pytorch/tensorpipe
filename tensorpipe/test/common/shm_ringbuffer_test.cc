/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/ringbuffer.h>
#include <tensorpipe/common/ringbuffer_role.h>
#include <tensorpipe/common/shm_ringbuffer.h>
#include <tensorpipe/common/shm_segment.h>
#include <tensorpipe/common/socket.h>

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe;

constexpr static int kNumRingbufferRoles = 2;
using Consumer = RingBufferRole<kNumRingbufferRoles, 0>;
using Producer = RingBufferRole<kNumRingbufferRoles, 1>;

// Same process produces and consumes share memory through different mappings.
TEST(ShmRingBuffer, SameProducerConsumer) {
  Fd headerFd;
  Fd dataFd;
  {
    // Producer part.
    // Buffer large enough to fit all data and persistent
    // (needs to be unlinked up manually).
    Error error;
    ShmSegment headerSegment;
    ShmSegment dataSegment;
    RingBuffer<kNumRingbufferRoles> rb;
    std::tie(error, headerSegment, dataSegment, rb) =
        createShmRingBuffer<kNumRingbufferRoles>(256 * 1024);
    Producer prod{rb};

    // Producer loop. It all fits in buffer.
    int i = 0;
    while (i < 2000) {
      ssize_t ret = prod.write(&i, sizeof(i));
      EXPECT_EQ(ret, sizeof(i));
      ++i;
    }

    // Duplicate the file descriptors so that the shared memory remains alive
    // when the original fds are closed by the segments' destructors.
    headerFd = Fd(::dup(headerSegment.getFd()));
    dataFd = Fd(::dup(dataSegment.getFd()));
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    Error error;
    ShmSegment headerSegment;
    ShmSegment dataSegment;
    RingBuffer<kNumRingbufferRoles> rb;
    std::tie(error, headerSegment, dataSegment, rb) =
        loadShmRingBuffer<kNumRingbufferRoles>(
            std::move(headerFd), std::move(dataFd));
    Consumer cons{rb};

    int i = 0;
    while (i < 2000) {
      int value;
      ssize_t ret = cons.read(&value, sizeof(value));
      EXPECT_EQ(ret, sizeof(value));
      EXPECT_EQ(value, i);
      ++i;
    }
  }
};

TEST(ShmRingBuffer, SingleProducer_SingleConsumer) {
  int sockFds[2];
  {
    int rv = socketpair(AF_UNIX, SOCK_STREAM, 0, sockFds);
    if (rv != 0) {
      TP_THROW_SYSTEM(errno) << "Failed to create socket pair";
    }
  }

  int eventFd = eventfd(0, 0);
  if (eventFd < 0) {
    TP_THROW_SYSTEM(errno) << "Failed to create event fd";
  }

  int pid = fork();
  if (pid < 0) {
    TP_THROW_SYSTEM(errno) << "Failed to fork";
  }

  if (pid == 0) {
    // child, the producer
    // Make a scope so segments are destroyed even on exit(0).
    {
      Error error;
      ShmSegment headerSegment;
      ShmSegment dataSegment;
      RingBuffer<kNumRingbufferRoles> rb;
      std::tie(error, headerSegment, dataSegment, rb) =
          createShmRingBuffer<kNumRingbufferRoles>(1024);
      Producer prod{rb};

      {
        auto err = sendFdsToSocket(
            sockFds[0], headerSegment.getFd(), dataSegment.getFd());
        if (err) {
          TP_THROW_ASSERT() << err.what();
        }
      }

      int i = 0;
      while (i < 2000) {
        ssize_t ret = prod.write(&i, sizeof(i));
        if (ret == -ENODATA) {
          std::this_thread::yield();
          continue;
        }
        EXPECT_EQ(ret, sizeof(i));
        ++i;
      }
      // Because of buffer size smaller than amount of data written,
      // producer cannot have completed the loop before consumer
      // started consuming the data.

      {
        uint64_t c;
        ::read(eventFd, &c, sizeof(uint64_t));
      }
    }
    // Child exits. Careful when calling exit() directly, because
    // it does not call destructors. We ensured shared_ptrs were
    // destroyed before by calling exit(0).
    exit(0);
  }
  // parent, the consumer

  // Wait for other process to create buffer.
  Fd headerFd;
  Fd dataFd;
  {
    auto err = recvFdsFromSocket(sockFds[1], headerFd, dataFd);
    if (err) {
      TP_THROW_ASSERT() << err.what();
    }
  }
  Error error;
  ShmSegment headerSegment;
  ShmSegment dataSegment;
  RingBuffer<kNumRingbufferRoles> rb;
  std::tie(error, headerSegment, dataSegment, rb) =
      loadShmRingBuffer<kNumRingbufferRoles>(
          std::move(headerFd), std::move(dataFd));
  Consumer cons{rb};

  int i = 0;
  while (i < 2000) {
    int value;
    ssize_t ret = cons.read(&value, sizeof(value));
    if (ret == -ENODATA) {
      std::this_thread::yield();
      continue;
    }
    EXPECT_EQ(ret, sizeof(value));
    EXPECT_EQ(value, i);
    ++i;
  }
  {
    uint64_t c = 1;
    ::write(eventFd, &c, sizeof(uint64_t));
  }
  ::close(eventFd);
  ::close(sockFds[0]);
  ::close(sockFds[1]);
  // Wait for child to make gtest happy.
  ::wait(nullptr);
};
