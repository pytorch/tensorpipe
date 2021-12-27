/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
// It will acquire a shared_ptr to the object (thus preventing the object from
// being destroyed until the callback has been fired) and in case of error it
// will deal with it but it will still end up invoking the actual callback.
template <typename TSubject>
class CallbackWrapper {
 public:
  CallbackWrapper(
      std::enable_shared_from_this<TSubject>& subject,
      DeferredExecutor& loop)
      : subject_(subject), loop_(loop) {}

  template <typename TBoundFn>
  auto operator()(TBoundFn fn) {
    return [this, subject{subject_.shared_from_this()}, fn{std::move(fn)}](
               const Error& error, auto&&... args) mutable {
      this->entryPoint(
          std::move(subject),
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
      std::shared_ptr<TSubject> subject,
      TBoundFn fn,
      const Error& error,
      Args&&... args) {
    // Do *NOT* move subject into the lambda's closure, as the shared_ptr we're
    // holding may be the last one keeping subject alive, in which case it would
    // die once the lambda runs, and it might kill the loop in turn too, _while_
    // the loop's deferToLoop method is running. That's bad. So copy it instead.
    // FIXME We're copying the args here...
    loop_.deferToLoop(
        [this, subject, fn{std::move(fn)}, error{error}, args...]() mutable {
          entryPointFromLoop(
              *subject, std::move(fn), error, std::forward<Args>(args)...);
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

} // namespace tensorpipe
