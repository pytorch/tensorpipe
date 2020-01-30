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
  return f(std::get<I>(std::move(t))...);
}

template <typename F, typename T>
auto cb_apply(F&& f, T&& t) {
  return cb_apply(
      std::move(f),
      std::move(t),
      std::make_index_sequence<std::tuple_size<T>::value>{});
}

} // namespace

// A queue of callbacks plus the arguments they must be invoked with. Typical
// usage involves one thread scheduling them to the queue once it intends for
// them to fire, and another thread actually running them.
template <typename F, typename... Args>
class CallbackQueue {
 public:
  void schedule(F&& fn, Args&&... args) {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.emplace_back(std::move(fn), std::tuple<Args...>(std::move(args)...));
  };

  void run() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!queue_.empty()) {
      F fn{std::move(std::get<0>(queue_.front()))};
      std::tuple<Args...> args{std::move(std::get<1>(queue_.front()))};
      queue_.pop_front();
      lock.unlock();
      // In C++17 use std::apply.
      cb_apply(std::move(fn), std::move(args));
      lock.lock();
    }
  }

 private:
  std::mutex mutex_;
  std::deque<std::tuple<F, std::tuple<Args...>>> queue_;
};

// A wrapper for a callback that "burns out" after it fires and thus needs to be
// rearmed every time. Invocations that are triggered while the callback is
// unarmed are stashed and will be delayed until a callback is provided again.
template <typename F, typename... Args>
class RearmableCallback {
 public:
  void arm(F&& f) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!queue_.empty()) {
      std::tuple<Args...> args{std::move(queue_.front())};
      queue_.pop_front();
      cb_apply(std::move(f), std::move(args));
    } else {
      callback_ = std::move(f);
    }
  };

  void trigger(Args&&... args) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_.has_value()) {
      F f{std::move(callback_.value())};
      callback_.reset();
      cb_apply(std::move(f), std::tuple<Args...>(std::move(args)...));
    } else {
      queue_.push_back(std::tuple<Args...>(std::move(args)...));
    }
  }

  // This method is intended for "flushing" the callback, for example when an
  // error condition is reached which means that no more callbacks will be
  // processed but the current ones still must be honored.
  void triggerIfArmed(Args&&... args) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_.has_value()) {
      F f{std::move(callback_.value())};
      callback_.reset();
      cb_apply(std::move(f), std::tuple<Args...>(std::move(args)...));
    }
  }

 private:
  std::mutex mutex_;
  optional<F> callback_;
  std::deque<std::tuple<Args...>> queue_;
};

} // namespace tensorpipe
