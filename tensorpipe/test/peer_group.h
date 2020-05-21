/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <thread>

#include <unistd.h>

#include <gtest/gtest.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>

class PeerGroup {
 public:
  static constexpr int kServer = 0;
  static constexpr int kClient = 1;

  virtual ~PeerGroup() = default;

  // Send message to given peer.
  virtual void send(int peerId, const std::string&) = 0;

  // Read next message for given peer. This method is blocking.
  virtual std::string recv(int peerId) = 0;

  // Spawn two peers each running one of the provided functions.
  virtual void spawn(std::function<void()>, std::function<void()>) = 0;

  // Signal other peers that this peer is done.
  void done(int peerId) {
    send(1 - peerId, kDone);
    std::unique_lock<std::mutex> lock(m_);
    done_[peerId] = true;
    condVar_[peerId].notify_one();
  }

  // Wait for all peers (including this one) to be done.
  void join(int peerId) {
    EXPECT_EQ(kDone, recv(peerId));

    std::unique_lock<std::mutex> lock(m_);
    condVar_[peerId].wait(lock, [&] { return done_[peerId]; });
  }

 private:
  const std::string kDone = "done";
  std::mutex m_;
  std::array<bool, 2> done_ = {false, false};
  std::array<std::condition_variable, 2> condVar_;
};

class ThreadPeerGroup : public PeerGroup {
 public:
  void send(int peerId, const std::string& str) override {
    q_[peerId].push(str);
  }

  std::string recv(int peerId) override {
    return q_[peerId].pop();
  }

  void spawn(std::function<void()> f1, std::function<void()> f2) override {
    std::array<std::function<void()>, 2> fns = {std::move(f1), std::move(f2)};
    std::array<std::thread, 2> ts;

    for (int i = 0; i < 2; ++i) {
      ts[i] = std::thread(fns[i]);
    }

    for (auto& t : ts) {
      t.join();
    }
  }

 private:
  std::array<tensorpipe::Queue<std::string>, 2> q_;
};

class ProcessPeerGroup : public PeerGroup {
 public:
  void send(int peerId, const std::string& str) override {
    uint64_t len = str.length();

    int ret;

    ret = write(pipefd_[peerId][1], &len, sizeof(len));
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to write to pipe";
    EXPECT_EQ(sizeof(len), ret);

    ret = write(pipefd_[peerId][1], str.c_str(), len);
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to write to pipe";
    EXPECT_EQ(len, ret);
  }

  std::string recv(int peerId) override {
    int ret;

    uint64_t len;
    ret = read(pipefd_[peerId][0], &len, sizeof(len));
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to read from pipe";
    EXPECT_EQ(sizeof(len), ret);

    std::string str(len, 0);
    ret = read(pipefd_[peerId][0], &str[0], len);
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to read from pipe";
    EXPECT_EQ(len, ret);

    return str;
  }

  void spawn(std::function<void()> f1, std::function<void()> f2) override {
    std::array<std::function<void()>, 2> fns = {std::move(f1), std::move(f2)};
    std::array<pid_t, 2> pids = {-1, -1};

    for (int i = 0; i < 2; ++i) {
      TP_THROW_SYSTEM_IF(pipe(pipefd_[i].data()) < 0, errno)
          << "Failed to create pipe";
    }

    for (int i = 0; i < 2; ++i) {
      pids[i] = fork();
      TP_THROW_SYSTEM_IF(pids[i] < 0, errno) << "Failed to fork";
      if (pids[i] == 0) {
        // Close writing end of our pipe.
        TP_THROW_SYSTEM_IF(close(pipefd_[i][1]) < 0, errno)
            << "Failed to close fd";
        // Close reading end of other pipe.
        TP_THROW_SYSTEM_IF(close(pipefd_[1 - i][0]) < 0, errno)
            << "Failed to close fd";

        fns[i]();

        std::exit(testing::Test::HasFailure());
      } else {
        // TODO: Print child PID.
      }
    }

    // Close all pipes in parent process.
    for (int i = 0; i < 2; ++i) {
      for (int j = 0; j < 2; ++j) {
        TP_THROW_SYSTEM_IF(close(pipefd_[i][j]) < 0, errno)
            << "Failed to close fd";
      }
    }

    for (int i = 0; i < 2; ++i) {
      int status;
      TP_THROW_SYSTEM_IF(waitpid(-1, &status, 0) < 0, errno)
          << "Failed to wait for child process";
      EXPECT_TRUE(WIFEXITED(status));
      const int exit_status = WEXITSTATUS(status);
      EXPECT_EQ(0, exit_status);
    }
  }

 private:
  std::array<std::array<int, 2>, 2> pipefd_;
};
