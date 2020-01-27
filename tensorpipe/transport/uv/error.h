#pragma once

#include <tensorpipe/transport/error.h>

namespace tensorpipe {
namespace transport {
namespace uv {

class UVError final : public BaseError {
 public:
  UVError(int error) : error_(error) {}

  std::string what() const override;

 private:
  int error_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
