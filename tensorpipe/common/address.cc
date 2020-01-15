#include <tensorpipe/common/address.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {

std::string getSchemeOfAddress(const std::string& addr) {
  std::string::size_type endOfScheme = addr.find("://");
  if (endOfScheme == std::string::npos) {
    TP_THROW_EINVAL() << "addr has no scheme: " << addr;
  }
  return addr.substr(0, endOfScheme);
}

} // namespace tensorpipe
