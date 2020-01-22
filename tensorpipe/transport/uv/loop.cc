#include <tensorpipe/transport/uv/loop.h>

#include <tensorpipe/transport/uv/macros.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::shared_ptr<Loop> Loop::create() {
  return std::make_shared<Loop>();
}

Loop::Loop()
    : loop_(std::make_unique<uv_loop_t>()),
      async_(std::make_unique<uv_async_t>()) {
  int rv;
  rv = uv_loop_init(loop_.get());
  TP_THROW_UV_IF(rv < 0, rv);
  rv = uv_async_init(loop_.get(), async_.get(), uv__async_cb);
  TP_THROW_UV_IF(rv < 0, rv);
  async_->data = this;
  thread_.reset(new std::thread(&Loop::loop, this));
}

Loop::~Loop() noexcept {
  // Thread must have been joined before destructing the loop.
  TP_DCHECK(!thread_);
  // Release resources associated with loop.
  auto rv = uv_loop_close(loop_.get());
  TP_DCHECK(rv == 0);
}

void Loop::join() {
  run([&] { uv_close(reinterpret_cast<uv_handle_t*>(async_.get()), nullptr); });

  // Wait for event loop thread to terminate.
  thread_->join();
  thread_.reset();
}

void Loop::run(std::function<void()> fn) {
  std::future<void> future;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    fns_.emplace_back(std::move(fn));
    future = fns_.back().p_.get_future();
    auto rv = uv_async_send(async_.get());
    TP_THROW_UV_IF(rv < 0, rv);
  }

  // Wait for function to run.
  future.get();
}

void Loop::loop() {
  auto rv = uv_run(loop_.get(), UV_RUN_DEFAULT);
  TP_THROW_ASSERT_IF(rv != 0)
      << ": uv_run returned with active handles or requests";
}

void Loop::Function::run() {
  try {
    fn_();
    p_.set_value();
  } catch (...) {
    p_.set_exception(std::current_exception());
  }
}

void Loop::uv__async_cb(uv_async_t* handle) {
  auto& loop = *reinterpret_cast<Loop*>(handle->data);
  loop.runFunctions();
}

void Loop::runFunctions() {
  decltype(fns_) fns;

  {
    std::unique_lock<std::mutex> lock(mutex_);
    std::swap(fns, fns_);
  }

  for (auto& fn : fns) {
    fn.run();
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
