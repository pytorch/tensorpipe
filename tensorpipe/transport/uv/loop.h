#pragma once

#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Loop final : public std::enable_shared_from_this<Loop> {
 public:
  static std::shared_ptr<Loop> create();

  explicit Loop();

  ~Loop() noexcept;

  void close();

  void run(std::function<void()> fn);

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

  // Event loop thread entry function.
  void loop();

  // Wrapper for a deferred function and its promise.
  struct Function {
    explicit Function(std::function<void()>&& fn) : fn_(fn) {}

    std::function<void()> fn_;
    std::promise<void> p_;

    // Run function and finalize promise.
    void run();
  };

  // List of deferred functions to run when the loop is ready.
  std::list<Function> fns_;

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
