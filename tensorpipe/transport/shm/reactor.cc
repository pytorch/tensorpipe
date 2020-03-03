/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/reactor.h>

#include <tensorpipe/util/ringbuffer/shm.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

void writeToken(TReactorProducer& producer, Reactor::TToken token) {
  for (;;) {
    auto rv = producer.write(&token, sizeof(token));
    if (rv == -EAGAIN) {
      std::this_thread::yield();
      continue;
    }
    TP_DCHECK_EQ(rv, sizeof(token));
    break;
  }
}

} // namespace

Reactor::Reactor() {
  int headerFd;
  int dataFd;
  std::shared_ptr<TReactorRingBuffer> rb;
  std::tie(headerFd, dataFd, rb) =
      util::ringbuffer::shm::create<TReactorRingBuffer>(kSize);
  headerFd_ = Fd(headerFd);
  dataFd_ = Fd(dataFd);
  consumer_.emplace(rb);
  producer_.emplace(rb);
  thread_ = std::thread(&Reactor::run, this);
}

Reactor::~Reactor() {
  done_.store(true);
  thread_.join();
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
  return token;
}

void Reactor::remove(TToken token) {
  std::unique_lock<std::mutex> lock(mutex_);
  functions_[token] = nullptr;
  reusableTokens_.insert(token);
}

void Reactor::trigger(TToken token) {
  std::unique_lock<std::mutex> lock(mutex_);
  writeToken(producer_.value(), token);
}

std::tuple<int, int> Reactor::fds() const {
  return std::make_tuple(headerFd_.fd(), dataFd_.fd());
}

void Reactor::run() {
  while (!done_.load()) {
    uint32_t token;
    auto ret = consumer_->copy(sizeof(token), &token);
    if (ret == -ENODATA) {
      std::this_thread::yield();
      continue;
    }

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

Reactor::Trigger::Trigger(Fd&& headerFd, Fd&& dataFd)
    : producer_(util::ringbuffer::shm::load<TReactorRingBuffer>(
          // The header and data segment objects take over ownership
          // of file descriptors. Release them to avoid double close.
          headerFd.release(),
          dataFd.release())) {}

void Reactor::Trigger::run(TToken token) {
  writeToken(producer_, token);
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
