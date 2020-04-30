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
#include <unordered_map>

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
class LocklessRearmableCallback {
  using TStoredArgs = std::tuple<typename std::remove_reference<Args>::type...>;

 public:
  void arm(F&& f) {
    if (!args_.empty()) {
      TStoredArgs args{std::move(args_.front())};
      args_.pop_front();
      cb_apply(std::move(f), std::move(args));
    } else {
      callbacks_.push_back(std::move(f));
    }
  };

  void trigger(Args... args) {
    if (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cb_apply(std::move(f), std::tuple<Args...>(std::forward<Args>(args)...));
    } else {
      args_.emplace_back(std::forward<Args>(args)...);
    }
  }

  // This method is intended for "flushing" the callback, for example when an
  // error condition is reached which means that no more callbacks will be
  // processed but the current ones still must be honored.
  void triggerAll(std::function<std::tuple<Args...>()> fn) {
    while (!callbacks_.empty()) {
      F f{std::move(callbacks_.front())};
      callbacks_.pop_front();
      cb_apply(std::move(f), fn());
    }
  }

 private:
  std::deque<F> callbacks_;
  std::deque<TStoredArgs> args_;
};

// This class provides some boilerplate that is used by the pipe, the listener
// and others when passing a callback to some lower-level component.
// It is called "lazy" because it will only acquire a weak_ptr to the object
// (thus allowing the object to be destroyed without the callback having fired)
// and because in case of error it will deal with it on its own and won't end up
// invoking the actual callback.
template <typename T, typename... Args>
class LazyCallbackWrapper {
 public:
  using TCallback = std::function<void(const Error&, Args...)>;
  using TBoundCallback = std::function<void(T&, Args...)>;

  LazyCallbackWrapper(std::enable_shared_from_this<T>& subject)
      : subject_(subject){};

  TCallback operator()(TBoundCallback fn) {
    return runIfAlive(
        subject_,
        std::function<void(T&, const Error&, Args...)>(
            [this, fn{std::move(fn)}](
                T& subject, const Error& error, Args... args) mutable {
              this->entryPoint_(
                  subject, std::move(fn), error, std::move(args)...);
            }));
  }

 private:
  std::enable_shared_from_this<T>& subject_;

  void entryPoint_(
      T& subject,
      TBoundCallback fn,
      const Error& error,
      Args... args) {
    // FIXME We're copying the args here...
    subject.deferToLoop_(
        [this, &subject, fn{std::move(fn)}, error, args...]() mutable {
          entryPointFromLoop_(
              subject, std::move(fn), error, std::move(args)...);
        });
  }

  void entryPointFromLoop_(
      T& subject,
      TBoundCallback fn,
      const Error& error,
      Args... args) {
    TP_DCHECK(subject.inLoop_());

    if (processError_(subject, error)) {
      return;
    }
    if (fn) {
      fn(subject, std::move(args)...);
    }
  }

  bool processError_(T& subject, const Error& error) {
    TP_DCHECK(subject.inLoop_());

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

    subject.handleError_();

    return true;
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
template <typename T, typename... Args>
class EagerCallbackWrapper {
 public:
  using TCallback = std::function<void(const Error&, Args...)>;
  using TBoundCallback = std::function<void(T&, Args...)>;

  EagerCallbackWrapper(std::enable_shared_from_this<T>& subject)
      : subject_(subject){};

  TCallback operator()(TBoundCallback fn) {
    return std::function<void(const Error&, Args...)>(
        [this, subject{subject_.shared_from_this()}, fn{std::move(fn)}](
            const Error& error, Args... args) mutable {
          this->entryPoint_(*subject, std::move(fn), error, std::move(args)...);
        });
  }

 private:
  std::enable_shared_from_this<T>& subject_;

  void entryPoint_(
      T& subject,
      TBoundCallback fn,
      const Error& error,
      Args... args) {
    // FIXME We're copying the args here...
    subject.deferToLoop_(
        [this, &subject, fn{std::move(fn)}, error, args...]() mutable {
          entryPointFromLoop_(
              subject, std::move(fn), error, std::move(args)...);
        });
  }

  void entryPointFromLoop_(
      T& subject,
      TBoundCallback fn,
      const Error& error,
      Args... args) {
    TP_DCHECK(subject.inLoop_());

    processError_(subject, error);
    // Proceed regardless of any error: this is why it's called "eager".
    if (fn) {
      fn(subject, std::move(args)...);
    }
  }

  void processError_(T& subject, const Error& error) {
    TP_DCHECK(subject.inLoop_());

    // Nothing to do if we already were in an error state or if there is no
    // error.
    if (subject.error_ || !error) {
      return;
    }

    // Otherwise enter the error state and do the cleanup.
    subject.error_ = error;

    subject.handleError_();
  }
};

// This class is designed to be installed on objects that, when closed, should
// in turn cause other objects to be closed too. This is the case for contexts,
// which close pipes, connections, listeners and channels.
// This class goes hand in hand with the one following it.
class ClosingEmitter {
 public:
  void subscribe(uintptr_t token, std::function<void()> fn) {
    std::unique_lock<std::mutex> lock(mutex_);
    receivers_.emplace(token, std::move(fn));
  }

  void unsubscribe(uintptr_t token) {
    std::unique_lock<std::mutex> lock(mutex_);
    receivers_.erase(token);
  }

  void close() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& it : receivers_) {
      it.second();
    }
  }

 private:
  // We need a mutex because at the moment the users of this class are accessing
  // it directly, without being proxied through a method of the object hosting
  // the emitter, and thus not being channeled through its event loop.
  std::mutex mutex_;
  std::unordered_map<uintptr_t, std::function<void()>> receivers_;
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
        token_, runIfAlive(subject, std::function<void(T&)>([](T& subject) {
                             subject.close();
                           })));
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
