#pragma once

#include <memory>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class Loop final {
 public:
  static std::shared_ptr<Loop> create();

  explicit Loop(std::unique_ptr<uv_loop_t>);

  ~Loop() noexcept;

  void close();

 private:
  std::unique_ptr<uv_loop_t> loop_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
