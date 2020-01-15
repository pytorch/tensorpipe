#include <tensorpipe/transport/uv/sockaddr.h>

#include <cstring>
#include <utility>

#include <tensorpipe/common/defs.h>

#include <uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

Sockaddr Sockaddr::createInetSockAddr(const std::string& str) {
  struct sockaddr_storage ss;
  int port = 0;
  std::string addrStr;
  std::string portStr;

  // Ensure the sockaddr_storage is zeroed, because we don't always
  // write to all fields in the `sockaddr_[in|in6]` structures.
  memset(&ss, 0, sizeof(ss));

  // If the input string is an IPv6 address with port, the address
  // itself must be wrapped with brackets.
  if (addrStr.empty()) {
    auto start = str.find("[");
    auto stop = str.find("]");
    if (start < stop && start != std::string::npos &&
        stop != std::string::npos) {
      addrStr = str.substr(start + 1, stop - (start + 1));
      if (stop + 1 < str.size() && str[stop + 1] == ':') {
        portStr = str.substr(stop + 2);
      }
    }
  }

  // If the input string is an IPv4 address with port, we expect
  // at least a single period and a single colon in the string.
  if (addrStr.empty()) {
    auto period = str.find(".");
    auto colon = str.find(":");
    if (period != std::string::npos && colon != std::string::npos) {
      addrStr = str.substr(0, colon);
      portStr = str.substr(colon + 1);
    }
  }

  // Fallback to using entire input string as address without port.
  if (addrStr.empty()) {
    addrStr = str;
  }

  // Parse port number if specified.
  if (!portStr.empty()) {
    port = std::stoi(portStr);
    if (port < 0 || port > std::numeric_limits<uint16_t>::max()) {
      TP_THROW_EINVAL() << str;
    }
  }

  // Try to convert an IPv4 address.
  {
    struct sockaddr_in* in = reinterpret_cast<struct sockaddr_in*>(&ss);
    auto rv = uv_inet_pton(AF_INET, addrStr.c_str(), &in->sin_addr);
    if (rv == 0) {
      in->sin_family = AF_INET;
      in->sin_port = ntohs(port);
      return Sockaddr(ss, sizeof(struct sockaddr_in));
    }
  }

  // Try to convert an IPv6 address.
  {
    struct sockaddr_in6* in6 = reinterpret_cast<struct sockaddr_in6*>(&ss);
    auto rv = uv_inet_pton(AF_INET6, addrStr.c_str(), &in6->sin6_addr);
    if (rv == 0) {
      in6->sin6_family = AF_INET6;
      in6->sin6_port = ntohs(port);
      return Sockaddr(ss, sizeof(struct sockaddr_in6));
    }
  }

  // Invalid address.
  TP_THROW_EINVAL() << str;
}

Sockaddr::Sockaddr(const struct sockaddr_storage& addr, socklen_t addrlen)
    : addr_(addr), addrlen_(addrlen) {}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
