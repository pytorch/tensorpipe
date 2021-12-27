/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/shm_segment.h>
#include <tensorpipe/common/socket.h>

#include <sys/eventfd.h>
#include <sys/socket.h>

#include <thread>

#include <gtest/gtest.h>

using namespace tensorpipe;

// Same process produces and consumes share memory through different mappings.
TEST(ShmSegment, SameProducerConsumer_Scalar) {
  // Set affinity of producer to CPU zero so that consumer only has to read from
  // that one CPU's buffer.
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);

  // This must stay alive for the file descriptor to remain open.
  Fd fd;
  {
    // Producer part.
    Error error;
    ShmSegment segment;
    int* myIntPtr;
    std::tie(error, segment, myIntPtr) = ShmSegment::create<int>();
    ASSERT_FALSE(error) << error.what();
    int& myInt = *myIntPtr;
    myInt = 1000;

    // Duplicate the file descriptor so that the shared memory remains alive
    // when the original fd is closed by the segment's destructor.
    fd = Fd(::dup(segment.getFd()));
  }

  {
    // Consumer part.
    // Map file again (to a different address) and consume it.
    Error error;
    ShmSegment segment;
    int* myIntPtr;
    std::tie(error, segment, myIntPtr) = ShmSegment::load<int>(std::move(fd));
    ASSERT_FALSE(error) << error.what();
    EXPECT_EQ(segment.getSize(), sizeof(int));
    EXPECT_EQ(*myIntPtr, 1000);
  }
};

TEST(ShmSegment, SingleProducer_SingleConsumer_Array) {
  size_t numFloats = 330000;

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
    // Make a scope so shared_ptr's are released even on exit(0).
    {
      // use huge pages in creation and not in loading. This should only affects
      // TLB overhead.
      Error error;
      ShmSegment segment;
      float* myFloats;
      std::tie(error, segment, myFloats) =
          ShmSegment::create<float[]>(numFloats);
      ASSERT_FALSE(error) << error.what();

      for (int i = 0; i < numFloats; ++i) {
        myFloats[i] = i;
      }

      {
        auto err = sendFdsToSocket(sockFds[0], segment.getFd());
        if (err) {
          TP_THROW_ASSERT() << err.what();
        }
      }
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
  Fd segmentFd;
  {
    auto err = recvFdsFromSocket(sockFds[1], segmentFd);
    if (err) {
      TP_THROW_ASSERT() << err.what();
    }
  }
  Error error;
  ShmSegment segment;
  float* myFloats;
  std::tie(error, segment, myFloats) =
      ShmSegment::load<float[]>(std::move(segmentFd));
  ASSERT_FALSE(error) << error.what();
  EXPECT_EQ(numFloats * sizeof(float), segment.getSize());
  for (int i = 0; i < numFloats; ++i) {
    EXPECT_EQ(myFloats[i], i);
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
