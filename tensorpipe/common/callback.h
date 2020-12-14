/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>

#include <tensorpipe/common/deferred_executor.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

// Given an object (actually, something that can produce a shared_ptr of that
// object) and a callable that takes a reference to such an object as its first
// argument, return another callable that does three things:
// - It holds a weak_ptr to the object, to avoid it being artificially kept
//   alive by the mere existence of the returned callable.
// - Once the returned callable is executed, it attempts to reacquire a
//   shared_ptr and if it fails (meaning the object has been destroyed) it does
//   not run the original callable (this feature is where this function takes
//   its name from).
// - It calls the original callable with a reference to object while holding on
//   to the shared_ptr to make sure the object doesn't get destroyed while the
//   callable is running.
template <typename TSubject, typename TBoundFn>
auto runIfAlive(
    std::enable_shared_from_this<TSubject>& subject,
    TBoundFn&& fn) {
  // In C++17 use weak_from_this().
  return [weak{std::weak_ptr<TSubject>(subject.shared_from_this())},
          fn{std::move(fn)}](auto&&... args) mutable {
    std::shared_ptr<TSubject> shared = weak.lock();
    if (shared) {
      fn(*shared, std::forward<decltype(args)>(args)...);
    }
  };
}

namespace {

// NOTE: This is an incomplete implementation of C++17's `std::apply`.
template <typename F, typename T, size_t... I>
auto cbApply(F&& f, T&& t, std::index_sequence<I...> /*unused*/) {
  return f(std::get<I>(std::forward<T>(t))...);
}

template <typename F, typename T>
auto cbApply(F&& f, T&& t) {
  return cbApply(
      std::move(f),
      std::forward<T>(t),
      std::make_index_sequence<std::tuple_size<T>::value>{});
}

} // namespace

// A wrapper for a callback that "burns out" after it fires and thus needs to be
// rearmed every time. Invocations that are triggered while the callback is
// unarmed are stashed and will be delayed until a callback is provided again.
template <typename... Args>
class RearmableCallback {
  using TFn = std::function<void(Args...)>;
  using TStoredArgs = std::tuple<typename std::remove_reference<Args>::type...>;

 public:
  void arm(TFn fn) {
    if (!args_.empty()) {
      TStoredArgs args{std::move(args_.front())};
      args_.pop_front();
      cbApply(std::move(fn), std::move(args));
    } else {
      callbacks_.push_back(std::move(fn));
    }
  }

  void trigger(Args... args) {
    if (!callbacks_.empty()) {
      TFn fn{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cbApply(std::move(fn), std::tuple<Args...>(std::forward<Args>(args)...));
    } else {
      args_.emplace_back(std::forward<Args>(args)...);
    }
  }

  // This method is intended for "flushing" the callback, for example when an
  // error condition is reached which means that no more callbacks will be
  // processed but the current ones still must be honored.
  void triggerAll(std::function<std::tuple<Args...>()> generator) {
    while (!callbacks_.empty()) {
      TFn fn{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cbApply(std::move(fn), generator());
    }
  }

 private:
  std::deque<TFn> callbacks_;
  std::deque<TStoredArgs> args_;
};

// This class provides some boilerplate that is used by the pipe, the listener
// and others when passing a callback to some lower-level component.
// It is called "lazy" because it will only acquire a weak_ptr to the object
// (thus allowing the object to be destroyed without the callback having fired)
// and because in case of error it will deal with it on its own and won't end up
// invoking the actual callback.
template <typename TSubject>
class LazyCallbackWrapper {
 public:
  LazyCallbackWrapper(
      std::enable_shared_from_this<TSubject>& subject,
      DeferredExecutor& loop)
      : subject_(subject), loop_(loop) {}

  template <typename TBoundFn>
  auto operator()(TBoundFn&& fn) {
    return runIfAlive(
        subject_,
        [this, fn{std::move(fn)}](
            TSubject& subject, const Error& error, auto&&... args) mutable {
          this->entryPoint(
              subject,
              std::move(fn),
              error,
              std::forward<decltype(args)>(args)...);
        });
  }

 private:
  std::enable_shared_from_this<TSubject>& subject_;
  DeferredExecutor& loop_;

  template <typename TBoundFn, typename... Args>
  void entryPoint(
      TSubject& subject,
      TBoundFn&& fn,
      const Error& error,
      Args&&... args) {
    // FIXME We're copying the args here...
    loop_.deferToLoop(
        [this, &subject, fn{std::move(fn)}, error, args...]() mutable {
          entryPointFromLoop(
              subject, std::move(fn), error, std::forward<Args>(args)...);
        });
  }

  template <typename TBoundFn, typename... Args>
  void entryPointFromLoop(
      TSubject& subject,
      TBoundFn fn,
      const Error& error,
      Args&&... args) {
    TP_DCHECK(loop_.inLoop());

    subject.setError(error);
    // Proceed only in case of success: this is why it's called "lazy".
    if (!subject.error_) {
      fn(subject, std::forward<Args>(args)...);
    }
  }
};

// This class is very similar to the above one: it provides some boilerplate
// that is used by the pipe, the listener and others when passing a callback to
// some lower-level component.
// It is called "eager" because it will acquire a shared_ptr to the object (thus
// preventing the object from being destroyed until the callback has been fired)
// and because in case of error it will deal with it but it will still end up
// invoking the actual callback.
// The use case for this class is when a resource was "acquired" (e.g., a buffer
// was passed to a transport) and it will be "released" by calling the callback.
template <typename TSubject>
class EagerCallbackWrapper {
 public:
  EagerCallbackWrapper(
      std::enable_shared_from_this<TSubject>& subject,
      DeferredExecutor& loop)
      : subject_(subject), loop_(loop) {}

  template <typename TBoundFn>
  auto operator()(TBoundFn&& fn) {
    return [this, subject{subject_.shared_from_this()}, fn{std::move(fn)}](
               const Error& error, auto&&... args) mutable {
      this->entryPoint(
          *subject,
          std::move(fn),
          error,
          std::forward<decltype(args)>(args)...);
    };
  }

 private:
  std::enable_shared_from_this<TSubject>& subject_;
  DeferredExecutor& loop_;

  template <typename TBoundFn, typename... Args>
  void entryPoint(
      TSubject& subject,
      TBoundFn&& fn,
      const Error& error,
      Args&&... args) {
    // FIXME We're copying the args here...
    loop_.deferToLoop(
        [this, &subject, fn{std::move(fn)}, error, args...]() mutable {
          entryPointFromLoop(
              subject, std::move(fn), error, std::forward<Args>(args)...);
        });
  }

  template <typename TBoundFn, typename... Args>
  void entryPointFromLoop(
      TSubject& subject,
      TBoundFn fn,
      const Error& error,
      Args&&... args) {
    TP_DCHECK(loop_.inLoop());

    subject.setError(error);
    // Proceed regardless of any error: this is why it's called "eager".
    fn(subject, std::forward<Args>(args)...);
  }
};

// This class is designed to be installed on objects that, when closed, should
// in turn cause other objects to be closed too. This is the case for contexts,
// which close pipes, connections, listeners and channels.
// This class goes hand in hand with the one following it.
class ClosingEmitter {
 public:
  void subscribe(uintptr_t token, std::function<void()> fn) {
    loop_.deferToLoop([this, token, fn{std::move(fn)}]() mutable {
      subscribeFromLoop(token, std::move(fn));
    });
  }

  void unsubscribe(uintptr_t token) {
    loop_.deferToLoop([this, token]() { unsubscribeFromLoop(token); });
  }

  void close() {
    loop_.deferToLoop([this]() { closeFromLoop(); });
  }

 private:
  // FIXME We should share the on-demand loop with the object owning the emitter
  OnDemandDeferredExecutor loop_;
  std::unordered_map<uintptr_t, std::function<void()>> receivers_;

  void subscribeFromLoop(uintptr_t token, std::function<void()> fn) {
    receivers_.emplace(token, std::move(fn));
  }

  void unsubscribeFromLoop(uintptr_t token) {
    receivers_.erase(token);
  }

  void closeFromLoop() {
    for (auto& it : receivers_) {
      it.second();
    }
  }
};

// This class is designed to be installed on objects that need to become closed
// when another object is closed. This is the case for pipes, connections,
// listeners and channels when contexts get closed.
// This class goes hand in hand with the previous one.
class ClosingReceiver {
 public:
  // T will be the context.
  template <typename T>
  ClosingReceiver(const std::shared_ptr<T>& object, ClosingEmitter& emitter)
      : emitter_(std::shared_ptr<ClosingEmitter>(object, &emitter)) {}

  // T will be the pipe, the connection or the channel.
  template <typename T>
  void activate(T& subject) {
    TP_DCHECK_EQ(token_, 0);
    token_ = reinterpret_cast<uint64_t>(&subject);
    TP_DCHECK_GT(token_, 0);
    emitter_->subscribe(
        token_, runIfAlive(subject, [](T& subject) { subject.close(); }));
  }

  ~ClosingReceiver() {
    if (token_ > 0) {
      emitter_->unsubscribe(token_);
    }
  }

 private:
  uintptr_t token_{0};
  std::shared_ptr<ClosingEmitter> emitter_;
};

} // namespace tensorpipe
