/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/reactor.h>

#include <tensorpipe/common/shm_ringbuffer.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

void writeToken(Reactor::Producer& producer, Reactor::TToken token) {
  for (;;) {
    auto rv = producer.write(&token, sizeof(token));
    if (rv == -EAGAIN) {
      // There's contention on the spin-lock, wait for it by retrying.
      std::this_thread::yield();
      continue;
    }
    if (rv == -ENODATA) {
      // The ringbuffer is full. Retrying should typically work, but might lead
      // to a deadlock if, for example, a reactor thread is trying to write a
      // token to its own ringbuffer, as then it would be stuck here and never
      // proceed to consume data from the ringbuffer. This could also happen
      // across multiple processes. This case seems remote enough, and a proper
      // solution rather complicated, that we're going to take that risk...
      std::this_thread::yield();
      continue;
    }
    TP_DCHECK_EQ(rv, sizeof(token));
    break;
  }
}

} // namespace

Reactor::Reactor() {
  Error error;
  std::tie(error, headerSegment_, dataSegment_, rb_) =
      createShmRingBuffer<kNumRingbufferRoles>(kSize);
  TP_THROW_ASSERT_IF(error)
      << "Couldn't allocate ringbuffer for reactor: " << error.what();

  startThread("TP_SHM_reactor");
}

void Reactor::close() {
  if (!closed_.exchange(true)) {
    stopBusyPolling();
  }
}

void Reactor::join() {
  close();

  if (!joined_.exchange(true)) {
    joinThread();
  }
}

Reactor::~Reactor() {
  join();
}

Reactor::TToken Reactor::add(TFunction fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  TToken token;

  // Either reuse a token or generate a new one.
  auto it = reusableTokens_.begin();
  if (it != reusableTokens_.end()) {
    token = *it;
    reusableTokens_.erase(it);
  } else {
    // If there are no reusable tokens, the next token is always equal
    // to the number of tokens in use + 1.
    token = functions_.size();
  }

  // Ensure there is enough space in the functions vector.
  if (functions_.size() <= token) {
    functions_.resize(token + 1);
  }

  functions_[token] = std::move(fn);

  functionCount_++;

  return token;
}

void Reactor::remove(TToken token) {
  std::unique_lock<std::mutex> lock(mutex_);
  functions_[token] = nullptr;
  reusableTokens_.insert(token);
  functionCount_--;
}

std::tuple<int, int> Reactor::fds() const {
  return std::make_tuple(headerSegment_.getFd(), dataSegment_.getFd());
}

bool Reactor::pollOnce() {
  Consumer reactorConsumer(rb_);
  uint32_t token;
  auto ret = reactorConsumer.read(&token, sizeof(token));
  if (ret == -ENODATA) {
    return false;
  }
  TP_THROW_SYSTEM_IF(ret < 0, -ret);

  TFunction fn;

  // Make copy of std::function so we don't need
  // to hold the lock while executing it.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    TP_DCHECK_LT(token, functions_.size());
    fn = functions_[token];
  }

  if (fn) {
    fn();
  }

  return true;
}

bool Reactor::readyToClose() {
  return functionCount_ == 0;
}

Reactor::Trigger::Trigger(Fd headerFd, Fd dataFd) {
  // The header and data segment objects take over ownership
  // of file descriptors. Release them to avoid double close.
  Error error;
  std::tie(error, headerSegment_, dataSegment_, rb_) =
      loadShmRingBuffer<kNumRingbufferRoles>(
          std::move(headerFd), std::move(dataFd));
  TP_THROW_ASSERT_IF(error)
      << "Couldn't access ringbuffer of remote reactor: " << error.what();
}

void Reactor::Trigger::run(TToken token) {
  Producer producer(rb_);
  writeToken(producer, token);
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
