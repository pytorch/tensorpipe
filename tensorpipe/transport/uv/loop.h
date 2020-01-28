#pragma once

#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Loop final : public std::enable_shared_from_this<Loop> {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Loop> create();

  explicit Loop(ConstructorToken);

  ~Loop() noexcept;

  void join();

  template <typename F>
  void run(F&& fn) {
    // If we're running on the event loop thread, run function
    // immediately. Otherwise, schedule it to be run by the event loop
    // thread and wake it up.
    if (std::this_thread::get_id() == thread_->get_id()) {
      fn();
    } else {
      // Must use a copyable wrapper around std::promise because
      // we use it from a std::function which must be copyable.
      auto promise = std::make_shared<std::promise<void>>();
      auto future = promise->get_future();
      defer([promise, fn{std::move(fn)}]() {
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

  void defer(std::function<void()> fn);

  uv_loop_t* ptr() {
    return loop_.get();
  }

  template <typename T, typename... Args>
  std::shared_ptr<T> createHandle(Args&&... args) {
    auto handle =
        std::make_shared<T>(shared_from_this(), std::forward<Args>(args)...);
    handle->leak();
    handle->init();
    return handle;
  }

  template <typename T, typename... Args>
  std::shared_ptr<T> createRequest(Args&&... args) {
    auto request =
        std::make_shared<T>(shared_from_this(), std::forward<Args>(args)...);
    request->leak();
    return request;
  }

 private:
  std::unique_ptr<uv_loop_t> loop_;
  std::unique_ptr<uv_async_t> async_;

  std::unique_ptr<std::thread> thread_;
  std::mutex mutex_;

  // Wake up the event loop.
  void wakeup();

  // Event loop thread entry function.
  void loop();

  // List of deferred functions to run when the loop is ready.
  std::vector<std::function<void()>> fns_;

  // This function is called by the event loop thread whenever
  // we have to run a number of deferred functions.
  static void uv__async_cb(uv_async_t* handle);

  // Companion function to uv__async_cb as member function
  // on the loop class.
  void runFunctions();
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
