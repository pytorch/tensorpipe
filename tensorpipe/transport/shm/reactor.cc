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

void writeToken(util::ringbuffer::Producer& producer, Reactor::TToken token) {
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
  std::shared_ptr<util::ringbuffer::RingBuffer> rb;
  std::tie(headerFd, dataFd, rb) = util::ringbuffer::shm::create(kSize);
  headerFd_ = Fd(headerFd);
  dataFd_ = Fd(dataFd);
  consumer_.emplace(rb);
  producer_.emplace(rb);
  deferredFunctionToken_ = add([this]() { handleDeferredFunctionFromLoop(); });
  thread_ = std::thread(&Reactor::run, this);
}

void Reactor::close() {
  bool wasClosed = false;
  closed_.compare_exchange_strong(wasClosed, true);
  if (!wasClosed) {
    // No need to wake up the reactor, since it is busy-waiting.
  }
}

void Reactor::join() {
  close();

  bool wasJoined = false;
  joined_.compare_exchange_strong(wasJoined, true);
  if (!wasJoined) {
    thread_.join();
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

void Reactor::trigger(TToken token) {
  std::unique_lock<std::mutex> lock(mutex_);
  writeToken(producer_.value(), token);
}

std::tuple<int, int> Reactor::fds() const {
  return std::make_tuple(headerFd_.fd(), dataFd_.fd());
}

void Reactor::run() {
  // Stop when another thread has asked the reactor the close and when
  // all functions have been removed except for the one used to defer.
  while (!closed_ || functionCount_ > 1) {
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
  TP_DCHECK(deferredFunctionList_.empty());
  remove(deferredFunctionToken_);
}

Reactor::Trigger::Trigger(Fd&& headerFd, Fd&& dataFd)
    : producer_(util::ringbuffer::shm::load(
          // The header and data segment objects take over ownership
          // of file descriptors. Release them to avoid double close.
          headerFd.release(),
          dataFd.release())) {}

void Reactor::Trigger::run(TToken token) {
  writeToken(producer_, token);
}

void Reactor::deferToLoop(TDeferredFunction fn) {
  std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
  deferredFunctionList_.push_back(std::move(fn));
  trigger(deferredFunctionToken_);
}

void Reactor::handleDeferredFunctionFromLoop() {
  std::unique_lock<std::mutex> lock(deferredFunctionMutex_);
  std::list<TDeferredFunction> fns;
  std::swap(fns, deferredFunctionList_);

  // Unlock before executing, because the function could try to
  // defer another function and that needs the lock.
  lock.unlock();
  for (auto& fn : fns) {
    fn();
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
