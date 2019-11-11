#include <tensorpipe/context/uv/loop.h>

#include <tensorpipe/context/uv/macros.h>

namespace tensorpipe {
namespace context {
namespace uv {

std::shared_ptr<Loop> Loop::create() {
  auto ptr = std::make_unique<uv_loop_t>();
  auto loop = std::make_shared<Loop>(std::move(ptr));
  auto rv = uv_loop_init(loop->loop_.get());
  UV_ASSERT(rv, "uv_loop_init");
  return loop;
}

Loop::Loop(std::unique_ptr<uv_loop_t> loop) : loop_(std::move(loop)) {}

Loop::~Loop() {
  if (loop_) {
    close();
  }
}

void Loop::close() {
  auto rv = uv_loop_close(loop_.get());
  UV_ASSERT(rv, "uv_loop_close");
  loop_.reset();
}

} // namespace uv
} // namespace context
} // namespace tensorpipe
