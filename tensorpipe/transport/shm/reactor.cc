/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/reactor.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/util/ringbuffer/shm.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

void writeToken(util::ringbuffer::Producer& producer, Reactor::TToken token) {
  for (;;) {
    auto rv = producer.write(&token, sizeof(token));
    if (rv == -EAGAIN) {
      // There's contention on the spin-lock, wait for it by retrying.
      std::this_thread::yield();
      continue;
    }
    if (rv == -ENOSPC) {
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
  std::tie(headerSegment_, dataSegment_, rb_) =
      util::ringbuffer::shm::create(kSize);

  startThread("TP_SHM_reactor");
}

void Reactor::close() {
  if (!closed_.exchange(true)) {
    // No need to wake up the reactor, since it is busy-waiting.
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

void Reactor::eventLoop() {
  util::ringbuffer::Consumer reactorConsumer(rb_);
  // Stop when another thread has asked the reactor the close and when
  // all functions have been removed.
  while (!closed_ || functionCount_ > 0) {
    uint32_t token;
    auto ret = reactorConsumer.read(&token, sizeof(token));
    if (ret == -ENODATA) {
      if (deferredFunctionCount_ > 0) {
        deferredFunctionCount_ -= runDeferredFunctionsFromEventLoop();
      } else {
        std::this_thread::yield();
      }
      continue;
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
  }
}

Reactor::Trigger::Trigger(Fd headerFd, Fd dataFd) {
  // The header and data segment objects take over ownership
  // of file descriptors. Release them to avoid double close.
  std::tie(headerSegment_, dataSegment_, rb_) =
      util::ringbuffer::shm::load(std::move(headerFd), std::move(dataFd));
}

void Reactor::Trigger::run(TToken token) {
  util::ringbuffer::Producer producer(rb_);
  writeToken(producer, token);
}

void Reactor::wakeupEventLoopToDeferFunction() {
  ++deferredFunctionCount_;
  // No need to wake up the reactor, since it is busy-waiting.
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
