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

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
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
// (This version of the class has its own lock which it uses to be thread safe)
template <typename F, typename... Args>
class RearmableCallbackWithOwnLock {
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

// A wrapper for a callback that "burns out" after it fires and thus needs to be
// rearmed every time. Invocations that are triggered while the callback is
// unarmed are stashed and will be delayed until a callback is provided again.
// (This version of the class forwards a user-provided lock to the callback)
template <typename F, typename... Args>
class RearmableCallbackWithExternalLock {
  using TLock = std::unique_lock<std::mutex>&;
  using TStoredArgs = std::tuple<typename std::remove_reference<Args>::type...>;

 public:
  void arm(F&& f, TLock lock) {
    TP_DCHECK(lock.owns_lock());
    if (!args_.empty()) {
      TStoredArgs args{std::move(args_.front())};
      args_.pop_front();
      cb_apply(
          std::move(f),
          std::tuple_cat(std::move(args), std::forward_as_tuple(lock)));
    } else {
      callbacks_.push_back(std::move(f));
    }
  };

  void trigger(Args... args, TLock lock) {
    TP_DCHECK(lock.owns_lock());
    if (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cb_apply(
          std::move(f),
          std::tuple<Args..., TLock>(std::forward<Args>(args)..., lock));
    } else {
      args_.emplace_back(std::forward<Args>(args)...);
    }
  }

  // This method is intended for "flushing" the callback, for example when an
  // error condition is reached which means that no more callbacks will be
  // processed but the current ones still must be honored.
  void triggerAll(std::function<std::tuple<Args...>()> fn, TLock lock) {
    TP_DCHECK(lock.owns_lock());
    while (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cb_apply(std::move(f), std::tuple_cat(fn(), std::forward_as_tuple(lock)));
    }
  }

 private:
  std::deque<F> callbacks_;
  std::deque<TStoredArgs> args_;
};

// This class provides some boilerplate that is used by the pipe, the listener
// and others when passing a callback to some lower-level component. It wraps
// the callback in runIfAlive, and adds locking and error handling.
template <typename T, typename... Args>
class CallbackWrapper {
 public:
  using TLock = std::unique_lock<std::mutex>&;
  using TCallback = std::function<void(const Error&, Args...)>;
  using TBoundCallback = std::function<void(T&, Args..., TLock)>;

  CallbackWrapper(std::enable_shared_from_this<T>& subject)
      : subject_(subject){};

  TCallback operator()(TBoundCallback callback) {
    return runIfAlive(
        subject_,
        std::function<void(T&, const Error&, Args...)>(
            [this, callback{std::move(callback)}](
                T& subject, const Error& error, Args... args) mutable {
              this->entryPoint_(
                  subject, std::move(callback), error, std::move(args)...);
            }));
  }

 private:
  std::enable_shared_from_this<T>& subject_;

  void entryPoint_(
      T& subject,
      TBoundCallback callback,
      const Error& error,
      Args... args) {
    std::unique_lock<std::mutex> lock(subject.mutex_);
    if (processError_(subject, error, lock)) {
      return;
    }
    if (callback) {
      callback(subject, std::move(args)..., lock);
    }
  }

  bool processError_(T& subject, const Error& error, TLock lock) {
    TP_DCHECK(lock.owns_lock() && lock.mutex() == &subject.mutex_);

    // Nothing to do if we already were in an error state or if there is no
    // error.
    if (subject.error_) {
      return true;
    }
    if (!error) {
      return false;
    }

    // Otherwise enter the error state and do the cleanup.
    subject.error_ = error;

    subject.handleError_(lock);

    return true;
  }
};

} // namespace tensorpipe
