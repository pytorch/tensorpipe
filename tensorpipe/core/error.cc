#include <tensorpipe/core/error.h>

#include <sstream>

namespace tensorpipe {

const Error Error::kSuccess = Error();

std::string TransportError::what() const {
  std::ostringstream ss;
  ss << "transport error: " << error_.what();
  return ss.str();
}

std::string LogicError::what() const {
  std::ostringstream ss;
  ss << "logic error: " << reason_;
  return ss.str();
}

} // namespace tensorpipe
