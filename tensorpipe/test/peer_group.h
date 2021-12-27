/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <string>
#include <thread>

#include <unistd.h>

#include <gtest/gtest.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/queue.h>

class PeerGroup {
 public:
  static constexpr int kNumPeers = 2;
  static constexpr int kServer = 0;
  static constexpr int kClient = 1;

  virtual ~PeerGroup() = default;

  // Send message to given peer.
  virtual void send(int receiverId, const std::string&) = 0;

  // Read next message for given peer. This method is blocking.
  virtual std::string recv(int receiverId) = 0;

  // Spawn two peers each running one of the provided functions.
  virtual void spawn(std::function<void()>, std::function<void()>) = 0;

  // Whether the two endpoints are two threads in the same process (as opposed
  // to two separate processes).
  virtual bool endpointsInSameProcess() const = 0;

  // Signal other peers that this peer is done.
  void done(int selfId) {
    send(1 - selfId, doneString_);
    std::unique_lock<std::mutex> lock(m_);
    done_[selfId] = true;
    condVar_[selfId].notify_one();
  }

  // Wait for all peers (including this one) to be done.
  void join(int selfId) {
    EXPECT_EQ(doneString_, recv(selfId));

    std::unique_lock<std::mutex> lock(m_);
    condVar_[selfId].wait(lock, [&] { return done_[selfId]; });
  }

 private:
  // This should be static but then we need to define it out-of-line (or mark it
  // as inline once we can use C++-17).
  const std::string doneString_ = "done";
  std::mutex m_;
  std::array<bool, kNumPeers> done_{{false, false}};
  std::array<std::condition_variable, kNumPeers> condVar_;
};

class ThreadPeerGroup : public PeerGroup {
 public:
  void send(int receiverId, const std::string& str) override {
    q_[receiverId].push(str);
  }

  std::string recv(int receiverId) override {
    return q_[receiverId].pop();
  }

  void spawn(std::function<void()> f1, std::function<void()> f2) override {
    std::array<std::function<void()>, kNumPeers> fns = {
        std::move(f1), std::move(f2)};
    std::array<std::thread, kNumPeers> ts;

    for (int peerId = 0; peerId < kNumPeers; ++peerId) {
      ts[peerId] = std::thread(fns[peerId]);
    }

    for (auto& t : ts) {
      t.join();
    }
  }

  bool endpointsInSameProcess() const override {
    return true;
  }

 private:
  std::array<tensorpipe::Queue<std::string>, kNumPeers> q_;
};

class ForkedThreadPeerGroup : public ThreadPeerGroup {
 public:
  void spawn(std::function<void()> f1, std::function<void()> f2) override {
    // Some tests modify the global state of the process (such as initializing
    // the CUDA context), which would cause other tests running as sub-processes
    // to fail. Here, we run all thread-based tests in a sub-process to avoid
    // this issue.
    pid_t pid = fork();
    TP_THROW_SYSTEM_IF(pid < 0, errno) << "Failed to fork";
    if (pid == 0) {
      ThreadPeerGroup::spawn(f1, f2);
      std::exit(((testing::Test::HasFailure()) ? 1 : 0));
    }

    int status;
    TP_THROW_SYSTEM_IF(waitpid(pid, &status, 0) < 0, errno)
        << "Failed to wait for child test process";
    EXPECT_TRUE(WIFEXITED(status));
    if (WIFSIGNALED(status)) {
      TP_LOG_WARNING() << "Test process terminated with signal "
                       << WTERMSIG(status);
    }
    const int exitStatus = WEXITSTATUS(status);
    EXPECT_EQ(0, exitStatus);
  }
};

class ProcessPeerGroup : public PeerGroup {
 public:
  void send(int receiverId, const std::string& str) override {
    uint64_t len = str.length();

    int ret;

    ret = write(pipefd_[receiverId][kWriteEnd], &len, sizeof(len));
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to write to pipe";
    EXPECT_EQ(sizeof(len), ret);

    ret = write(pipefd_[receiverId][kWriteEnd], str.data(), len);
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to write to pipe";
    EXPECT_EQ(len, ret);
  }

  std::string recv(int receiverId) override {
    int ret;

    uint64_t len;
    ret = read(pipefd_[receiverId][kReadEnd], &len, sizeof(len));
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to read from pipe";
    EXPECT_EQ(sizeof(len), ret);

    std::string str(len, 0);
    ret = read(pipefd_[receiverId][kReadEnd], &str[0], len);
    TP_THROW_SYSTEM_IF(ret < 0, errno) << "Failed to read from pipe";
    EXPECT_EQ(len, ret);

    return str;
  }

  void spawn(std::function<void()> f1, std::function<void()> f2) override {
    std::array<std::function<void()>, kNumPeers> fns = {
        std::move(f1), std::move(f2)};
    std::array<pid_t, kNumPeers> pids = {-1, -1};

    for (int peerId = 0; peerId < kNumPeers; ++peerId) {
      TP_THROW_SYSTEM_IF(pipe(pipefd_[peerId].data()) < 0, errno)
          << "Failed to create pipe";
    }

    for (int peerId = 0; peerId < kNumPeers; ++peerId) {
      pids[peerId] = fork();
      TP_THROW_SYSTEM_IF(pids[peerId] < 0, errno) << "Failed to fork";
      if (pids[peerId] == 0) {
        try {
          // Close writing end of our pipe.
          TP_THROW_SYSTEM_IF(close(pipefd_[peerId][kWriteEnd]) < 0, errno)
              << "Failed to close fd";
          // Close reading end of other pipe.
          TP_THROW_SYSTEM_IF(close(pipefd_[1 - peerId][kReadEnd]) < 0, errno)
              << "Failed to close fd";

          fns[peerId]();
        } catch (const std::exception& e) {
          TP_LOG_ERROR() << "Child #" << peerId << " (PID " << getpid()
                         << ") encountered exception " << e.what();
          std::exit(2);
        } catch (...) {
          std::exit(3);
        }
        std::exit(((testing::Test::HasFailure()) ? 1 : 0));
      }
    }

    // Close all pipes in parent process.
    for (int peerId = 0; peerId < kNumPeers; ++peerId) {
      for (int pipeEnd = 0; pipeEnd < 2; ++pipeEnd) {
        TP_THROW_SYSTEM_IF(close(pipefd_[peerId][pipeEnd]) < 0, errno)
            << "Failed to close fd";
      }
    }

    for (int peerId = 0; peerId < kNumPeers; ++peerId) {
      int status;
      TP_THROW_SYSTEM_IF(waitpid(-1, &status, 0) < 0, errno)
          << "Failed to wait for child process";
      EXPECT_TRUE(WIFEXITED(status));
      if (WIFSIGNALED(status)) {
        TP_LOG_WARNING() << "Peer process terminated with signal "
                         << WTERMSIG(status);
      }
      const int exitStatus = WEXITSTATUS(status);
      EXPECT_EQ(0, exitStatus);
    }
  }

  bool endpointsInSameProcess() const override {
    return false;
  }

 private:
  static constexpr int kReadEnd = 0;
  static constexpr int kWriteEnd = 1;

  std::array<std::array<int, 2>, kNumPeers> pipefd_;
};
