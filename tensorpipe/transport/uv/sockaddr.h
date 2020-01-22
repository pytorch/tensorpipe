#pragma once

#include <sys/socket.h>

#include <string>

namespace tensorpipe {
namespace transport {
namespace uv {

class Sockaddr final {
 public:
  static Sockaddr createInetSockAddr(const std::string& name);

  Sockaddr(const struct sockaddr_storage& addr, socklen_t addrlen)
      : addr_(addr), addrlen_(addrlen) {}

  inline const struct sockaddr* addr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline struct sockaddr* addr() {
    return reinterpret_cast<struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const {
    return addrlen_;
  }

  std::string str() const;

 private:
  struct sockaddr_storage addr_;
  socklen_t addrlen_;
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
