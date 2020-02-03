/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/socket.h>
#include <tensorpipe/util/shm/segment.h>

#include <sys/eventfd.h>
#include <sys/socket.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe::util::shm;
using namespace tensorpipe::transport::shm;

// Same process produces and consumes share memory through different mappings.
TEST(Segment, SameProducerConsumer_Scalar) {
  // Set affinity of producer to CPU zero so that consumer only has to read from
  // that one CPU's buffer.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

  // This must stay alive for the file descriptor to remain open.
  std::shared_ptr<Segment> producer_segment;
  {
    // Producer part.
    std::shared_ptr<int> my_int_ptr;
    std::tie(my_int_ptr, producer_segment) =
        Segment::create<int>(true, PageType::Default);
    int& my_int = *my_int_ptr;
    my_int = 1000;
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    std::shared_ptr<int> my_int_ptr;
    std::shared_ptr<Segment> segment;
    std::tie(my_int_ptr, segment) =
        Segment::load<int>(producer_segment->getFd(), true, PageType::Default);
    EXPECT_EQ(segment->getSize(), sizeof(int));
    EXPECT_EQ(*my_int_ptr, 1000);
  }
};

TEST(SegmentManager, SingleProducer_SingleConsumer_Array) {
  size_t num_floats = 330000;

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
    // Make a scope so shared_ptr's are released even on exit(0).
    {
      // use huge pages in creation and not in loading. This should only affects
      // TLB overhead.
      std::shared_ptr<float> my_floats;
      std::shared_ptr<Segment> segment;
      std::tie(my_floats, segment) =
          Segment::create<float[]>(num_floats, true, PageType::HugeTLB_2MB);

      for (int i = 0; i < num_floats; ++i) {
        my_floats.get()[i] = i;
      }

      {
        auto err = sendFdsToSocket(sock_fds[0], segment->getFd());
        if (err) {
          TP_THROW_ASSERT() << err.what();
        }
      }
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
  int segment_fd;
  {
    auto err = recvFdsFromSocket(sock_fds[1], segment_fd);
    if (err) {
      TP_THROW_ASSERT() << err.what();
    }
  }
  std::shared_ptr<float> my_floats;
  std::shared_ptr<Segment> segment;
  std::tie(my_floats, segment) =
      Segment::load<float[]>(segment_fd, false, PageType::Default);
  EXPECT_EQ(num_floats * sizeof(float), segment->getSize());
  for (int i = 0; i < num_floats; ++i) {
    EXPECT_EQ(my_floats.get()[i], i);
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
