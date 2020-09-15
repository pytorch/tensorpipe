/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/socket.h>
#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>
#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/ringbuffer/shm.h>
#include <tensorpipe/util/shm/segment.h>

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::util::ringbuffer;
using namespace tensorpipe::util::shm;

// Same process produces and consumes share memory through different mappings.
TEST(ShmRingBuffer, SameProducerConsumer) {
  Fd header_fd;
  Fd data_fd;
  {
    // Producer part.
    // Buffer large enough to fit all data and persistent
    // (needs to be unlinked up manually).
    Segment header_segment;
    Segment data_segment;
    RingBuffer rb;
    std::tie(header_segment, data_segment, rb) = shm::create(256 * 1024);
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
    header_fd = Fd(::dup(header_segment.getFd()));
    data_fd = Fd(::dup(data_segment.getFd()));
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    Segment header_segment;
    Segment data_segment;
    RingBuffer rb;
    std::tie(header_segment, data_segment, rb) =
        shm::load(std::move(header_fd), std::move(data_fd));
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
  int sock_fds[2];
  {
    int rv = socketpair(AF_UNIX, SOCK_STREAM, 0, sock_fds);
    if (rv != 0) {
      TP_THROW_SYSTEM(errno) << "Failed to create socket pair";
    }
  }

  int event_fd = eventfd(0, 0);
  if (event_fd < 0) {
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
      Segment header_segment;
      Segment data_segment;
      RingBuffer rb;
      std::tie(header_segment, data_segment, rb) = shm::create(1024);
      Producer prod{rb};

      {
        auto err = sendFdsToSocket(
            sock_fds[0], header_segment.getFd(), data_segment.getFd());
        if (err) {
          TP_THROW_ASSERT() << err.what();
        }
      }

      int i = 0;
      while (i < 2000) {
        ssize_t ret = prod.write(&i, sizeof(i));
        if (ret == -ENOSPC) {
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
        ::read(event_fd, &c, sizeof(uint64_t));
      }
    }
    // Child exits. Careful when calling exit() directly, because
    // it does not call destructors. We ensured shared_ptrs were
    // destroyed before by calling exit(0).
    exit(0);
  }
  // parent, the consumer

  // Wait for other process to create buffer.
  Fd header_fd;
  Fd data_fd;
  {
    auto err = recvFdsFromSocket(sock_fds[1], header_fd, data_fd);
    if (err) {
      TP_THROW_ASSERT() << err.what();
    }
  }
  Segment header_segment;
  Segment data_segment;
  RingBuffer rb;
  std::tie(header_segment, data_segment, rb) =
      shm::load(std::move(header_fd), std::move(data_fd));
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
    ::write(event_fd, &c, sizeof(uint64_t));
  }
  ::close(event_fd);
  ::close(sock_fds[0]);
  ::close(sock_fds[1]);
  // Wait for child to make gtest happy.
  ::wait(nullptr);
};
