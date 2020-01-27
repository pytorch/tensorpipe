#include <tensorpipe/transport/uv/error.h>

#include <sstream>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::string UVError::what() const {
  std::ostringstream ss;
  ss << uv_err_name(error_) << ": " << uv_strerror(error_);
  return ss.str();
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
