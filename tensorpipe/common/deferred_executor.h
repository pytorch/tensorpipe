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
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace tensorpipe {

// Dealing with thread-safety using per-object mutexes is prone to deadlocks
// because of reentrant calls (both "upward", when invoking a callback that
// calls back into a method of the object, and "downward", when passing a
// callback to an operation of another object that calls it inline) and lock
// inversions (object A calling a method of object B and attempting to acquire
// its lock, with the reverse happening at the same time). Using a "loop" model,
// where operations aren't called inlined and piled up on the stack but instead
// deferred to a later iteration of the loop, solves many of these issues. This
// abstract interface defines the essential methods we need such event loops to
// provide.
class DeferredExecutor {
 public:
  using TTask = std::function<void()>;

  virtual void deferToLoop(TTask fn) = 0;

  virtual bool inLoop() = 0;

  // Prefer using deferToLoop over runInLoop when you don't need to wait for the
  // result.
  template <typename F>
  void runInLoop(F&& fn) {
    // When called from the event loop thread itself (e.g., from a callback),
    // deferring would cause a deadlock because the given callable can only be
    // run when the loop is allowed to proceed. On the other hand, it means it
    // is thread-safe to run it immediately. The danger here however is that it
    // can lead to an inconsistent order between operations run from the event
    // loop, from outside of it, and deferred.
    if (inLoop()) {
      fn();
    } else {
      // Must use a copyable wrapper around std::promise because
      // we use it from a std::function which must be copyable.
      auto promise = std::make_shared<std::promise<void>>();
      auto future = promise->get_future();
      deferToLoop([promise, fn{std::forward<F>(fn)}]() {
        try {
          fn();
          promise->set_value();
        } catch (...) {
          promise->set_exception(std::current_exception());
        }
      });
      future.get();
    }
  }

  virtual ~DeferredExecutor() = default;
};

// Transports typically have their own thread they can use as deferred executors
// but many objects (like pipes) don't naturally own threads and introducing
// them would also mean introducing latency costs due to context switching.
// In order to give these objects a loop they can use to defer their operations
// to, we can have them temporarily hijack the calling thread and repurpose it
// to run an ephemeral loop on which to run the original task and all the ones
// that a task running on the loop chooses to defer to a later iteration of the
// loop, recursively. Once all these tasks have been completed, the makeshift
// loop is dismantled and control of the thread is returned to the caller.
// FIXME Rename this to OnDemandDeferredExecutor?
class OnDemandDeferredExecutor : public DeferredExecutor {
 public:
  bool inLoop() override {
    // If the current thread is already holding the lock (i.e., it's already in
    // this function somewhere higher up in the stack) then this check won't
    // race and we will detect it correctly. If this is not the case, then this
    // check may race with another thread, but that's nothing to worry about
    // because in either case the outcome will be negative.
    return currentLoop_ == std::this_thread::get_id();
  }

  void deferToLoop(TTask fn) override {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      pendingTasks_.push_back(std::move(fn));
      if (currentLoop_ != std::thread::id()) {
        return;
      }
      currentLoop_ = std::this_thread::get_id();
    }

    while (true) {
      TTask task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pendingTasks_.empty()) {
          currentLoop_ = std::thread::id();
          return;
        }
        task = std::move(pendingTasks_.front());
        pendingTasks_.pop_front();
      }
      task();
    }
  }

 private:
  std::mutex mutex_;
  std::atomic<std::thread::id> currentLoop_{std::thread::id()};
  std::deque<TTask> pendingTasks_;
};

} // namespace tensorpipe
