#include <tensorpipe/transport/error.h>

#include <cstring>
#include <sstream>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {

const Error Error::kSuccess = Error();

std::string Error::what() const {
  TP_DCHECK(error_);
  return error_->what();
}

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

std::string EOFError::what() const {
  std::ostringstream ss;
  ss << "eof";
  return ss.str();
}

} // namespace transport
} // namespace tensorpipe
