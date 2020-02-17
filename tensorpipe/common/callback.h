/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>

#include <tensorpipe/common/optional.h>

namespace tensorpipe {

// Wrap std::function such that it is only invoked if the object of type T is
// still alive when the function is called. If T has been destructed in the mean
// time, the std::function does not run.
template <typename T, typename... Args>
std::function<void(Args...)> runIfAlive(
    std::enable_shared_from_this<T>& subject,
    std::function<void(T&, Args...)> fn) {
  // In C++17 use weak_from_this().
  return [weak{std::weak_ptr<T>(subject.shared_from_this())},
          fn{std::move(fn)}](Args... args) {
    auto shared = weak.lock();
    if (shared) {
      fn(*shared, std::forward<Args>(args)...);
    }
  };
}

namespace {

// NOTE: This is an incomplete implementation of C++17's `std::apply`.
template <typename F, typename T, size_t... I>
auto cb_apply(F&& f, T&& t, std::index_sequence<I...>) {
  return f(std::get<I>(std::forward<T>(t))...);
}

template <typename F, typename T>
auto cb_apply(F&& f, T&& t) {
  return cb_apply(
      std::move(f),
      std::forward<T>(t),
      std::make_index_sequence<std::tuple_size<T>::value>{});
}

} // namespace

// A wrapper for a callback that "burns out" after it fires and thus needs to be
// rearmed every time. Invocations that are triggered while the callback is
// unarmed are stashed and will be delayed until a callback is provided again.
template <typename F, typename... Args>
class RearmableCallback {
  using TStoredArgs = std::tuple<typename std::remove_reference<Args>::type...>;

 public:
  void arm(F&& f) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!args_.empty()) {
      TStoredArgs args{std::move(args_.front())};
      args_.pop_front();
      lock.unlock();
      cb_apply(std::move(f), std::move(args));
    } else {
      callbacks_.push_back(std::move(f));
    }
  };

  void trigger(Args... args) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      lock.unlock();
      cb_apply(std::move(f), std::tuple<Args...>(std::forward<Args>(args)...));
    } else {
      args_.emplace_back(std::forward<Args>(args)...);
    }
  }

  // This method is intended for "flushing" the callback, for example when an
  // error condition is reached which means that no more callbacks will be
  // processed but the current ones still must be honored.
  void triggerAll(std::function<std::tuple<Args...>()> fn) {
    std::lock_guard<std::mutex> lock(mutex_);
    while (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cb_apply(std::move(f), fn());
    }
  }

 private:
  std::mutex mutex_;
  std::deque<F> callbacks_;
  std::deque<TStoredArgs> args_;
};

} // namespace tensorpipe
