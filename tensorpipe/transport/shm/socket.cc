#include <tensorpipe/transport/shm/socket.h>

#include <fcntl.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Sockaddr Sockaddr::createAbstractUnixAddr(const std::string& name) {
  struct sockaddr_un sun;
  sun.sun_family = AF_UNIX;
  std::memset(&sun.sun_path, 0, sizeof(sun.sun_path));
  constexpr size_t offset = 1;
  const size_t len = std::min(sizeof(sun.sun_path) - offset, name.size());
  std::strncpy(&sun.sun_path[offset], name.c_str(), len);

  // Note: instead of using sizeof(sun) we compute the addrlen from
  // the string length of the abstract socket name. If we use
  // sizeof(sun), lsof shows all the trailing NUL characters.
  return Sockaddr(
      reinterpret_cast<struct sockaddr*>(&sun),
      sizeof(sun.sun_family) + offset + len + 1);
};

Sockaddr::Sockaddr(struct sockaddr* addr, socklen_t addrlen) {
  TP_ARG_CHECK_LE(addrlen, sizeof(addr_));
  std::memcpy(&addr_, addr, addrlen);
  addrlen_ = addrlen;
}

std::shared_ptr<Socket> Socket::createForFamily(sa_family_t ai_family) {
  auto rv = socket(ai_family, SOCK_STREAM | SOCK_NONBLOCK, 0);
  TP_THROW_SYSTEM_IF(rv == -1, errno);
  return std::make_shared<Socket>(rv);
}

void Socket::block(bool on) {
  int rv;
  rv = fcntl(fd_, F_GETFL);
  TP_THROW_SYSTEM_IF(rv == -1, errno);
  if (!on) {
    // Set O_NONBLOCK
    rv |= O_NONBLOCK;
  } else {
    // Clear O_NONBLOCK
    rv &= ~O_NONBLOCK;
  }
  rv = fcntl(fd_, F_SETFL, rv);
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

void Socket::configureTimeout(int opt, std::chrono::milliseconds timeout) {
  struct timeval tv = {
      .tv_sec = timeout.count() / 1000,
      .tv_usec = (timeout.count() % 1000) * 1000,
  };
  auto rv = setsockopt(fd_, SOL_SOCKET, opt, &tv, sizeof(tv));
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

void Socket::recvTimeout(std::chrono::milliseconds timeout) {
  configureTimeout(SO_RCVTIMEO, std::move(timeout));
}

void Socket::sendTimeout(std::chrono::milliseconds timeout) {
  configureTimeout(SO_SNDTIMEO, std::move(timeout));
}

void Socket::bind(const Sockaddr& addr) {
  auto rv = ::bind(fd_, addr.addr(), addr.addrlen());
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

void Socket::listen(int backlog) {
  auto rv = ::listen(fd_, backlog);
  TP_THROW_SYSTEM_IF(rv == -1, errno);
}

std::shared_ptr<Socket> Socket::accept() {
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof(addr);
  int rv = -1;
  for (;;) {
    rv = ::accept(fd_, (struct sockaddr*)&addr, &addrlen);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      // Return empty shared_ptr to indicate failure.
      // The caller can assume errno has been set.
      return std::shared_ptr<Socket>();
    }
    break;
  }
  return std::make_shared<Socket>(rv);
}

void Socket::connect(const Sockaddr& addr) {
  for (;;) {
    auto rv = ::connect(fd_, addr.addr(), addr.addrlen());
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      if (errno != EINPROGRESS) {
        TP_THROW_SYSTEM(errno);
      }
    }
    break;
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
