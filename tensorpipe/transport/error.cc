#include <tensorpipe/transport/error.h>

#include <cstring>
#include <sstream>

namespace tensorpipe {
namespace transport {

const Error Error::kSuccess = Error();

std::string SystemError::what() const {
  std::ostringstream ss;
  ss << syscall_ << ": " << strerror(error_);
  return ss.str();
}

std::string ShortReadError::what() const {
  std::ostringstream ss;
  ss << "short read: got " << actual_ << " bytes while expecting to read "
     << expected_ << " bytes";
  return ss.str();
}

std::string ShortWriteError::what() const {
  std::ostringstream ss;
  ss << "short write: wrote " << actual_ << " bytes while expecting to write "
     << expected_ << " bytes";
  return ss.str();
}

} // namespace transport
} // namespace tensorpipe
