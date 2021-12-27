/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <sys/types.h>
#include <unistd.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/transport/shm/reactor.h>

#include <gtest/gtest.h>

using namespace tensorpipe;
using namespace tensorpipe::transport::shm;

namespace {

void run(std::function<void(int)> fn1, std::function<void(int)> fn2) {
  int fds[2];

  {
    auto rv = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (rv != 0) {
      TP_THROW_SYSTEM(errno) << "Failed to create socket pair";
    }
  }

  {
    auto pid = fork();
    TP_DCHECK_GE(pid, 0);
    if (pid == 0) {
      close(fds[0]);
      fn2(fds[1]);
      close(fds[1]);
      exit(0);
    }
  }

  close(fds[1]);
  fn1(fds[0]);
  close(fds[0]);
  wait(nullptr);
}

} // namespace

TEST(ShmReactor, Basic) {
  run(
      [](int fd) {
        tensorpipe::Queue<int> queue;
        auto reactor = std::make_shared<Reactor>();
        auto token1 = reactor->add([&] { queue.push(1); });
        auto token2 = reactor->add([&] { queue.push(2); });

        // Share reactor fds and token with other process.
        {
          auto socket = Socket(fd);
          auto fds = reactor->fds();
          auto error = socket.sendPayloadAndFds(
              token1, token2, std::get<0>(fds), std::get<1>(fds));
          ASSERT_FALSE(error) << error.what();
        }

        // Wait for other process to run trigger.
        ASSERT_EQ(queue.pop(), 1);
        ASSERT_EQ(queue.pop(), 2);

        reactor->remove(token1);
        reactor->remove(token2);
      },
      [](int fd) {
        Reactor::TToken token1;
        Reactor::TToken token2;
        Fd header;
        Fd data;

        // Wait for other process to share reactor fds and token.
        {
          auto socket = Socket(fd);
          auto error = socket.recvPayloadAndFds(token1, token2, header, data);
          ASSERT_FALSE(error) << error.what();
        }

        // Create and run trigger. This should wake up the other
        // process and run the registered function.
        Reactor::Trigger trigger(std::move(header), std::move(data));
        trigger.run(token1);
        trigger.run(token2);
      });
}

TEST(ShmReactor, TokenReuse) {
  tensorpipe::Queue<int> queue(3);
  auto reactor = std::make_shared<Reactor>();
  auto t1 = reactor->add([&] { queue.push(1); });
  auto t2 = reactor->add([&] { queue.push(2); });
  auto t3 = reactor->add([&] { queue.push(3); });

  // Check that they're monotonically increasing.
  ASSERT_GT(t2, t1);
  ASSERT_GT(t3, t2);

  // Remove token and check that it is reused.
  reactor->remove(t1);
  auto t4 = reactor->add([&] { queue.push(4); });
  ASSERT_EQ(t4, t1);

  // Remove multiple tokens and check that they're reused in order.
  reactor->remove(t2);
  reactor->remove(t3);
  auto t5 = reactor->add([&] { queue.push(5); });
  auto t6 = reactor->add([&] { queue.push(6); });
  ASSERT_EQ(t5, t2);
  ASSERT_EQ(t6, t3);

  reactor->remove(t4);
  reactor->remove(t5);
  reactor->remove(t6);
}
