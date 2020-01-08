#include <tensorpipe/transport/shm/socket.h>

#include <fcntl.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/error_macros.h>

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

Error Socket::sendFd(const Fd& fd) {
  using TPayload = int;

  // Build control message.
  std::array<char, CMSG_SPACE(sizeof(TPayload))> buf;
  struct cmsghdr* cmsg = reinterpret_cast<struct cmsghdr*>(buf.data());
  cmsg->cmsg_len = CMSG_LEN(sizeof(TPayload));
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  *reinterpret_cast<int*>(CMSG_DATA(cmsg)) = fd.fd();

  // Build dummy iov with a single NUL byte.
  struct iovec iov[1];
  char nul = 0;
  iov[0].iov_base = &nul;
  iov[0].iov_len = sizeof(nul);

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_iov = iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
  msg.msg_control = cmsg;
  msg.msg_controllen = CMSG_LEN(sizeof(TPayload));
  msg.msg_flags = 0;

  // Send message.
  for (;;) {
    auto rv = ::sendmsg(fd_, &msg, 0);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      return TP_CREATE_ERROR(SystemError, "sendmsg", errno);
    }
    if (rv != iov[0].iov_len) {
      return TP_CREATE_ERROR(ShortWriteError, iov[0].iov_len, rv);
    }
    break;
  }

  return Error::kSuccess;
}

Error Socket::recvFd(optional<Fd>* fd) {
  using TPayload = int;

  // Build control message.
  std::array<char, CMSG_SPACE(sizeof(TPayload))> buf;
  struct cmsghdr* cmsg = reinterpret_cast<struct cmsghdr*>(buf.data());
  cmsg->cmsg_len = CMSG_LEN(sizeof(TPayload));
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;

  // Build dummy iov with a single NUL byte.
  struct iovec iov[1];
  char nul = 0;
  iov[0].iov_base = &nul;
  iov[0].iov_len = sizeof(nul);

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_iov = iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iov[0]);
  msg.msg_control = cmsg;
  msg.msg_controllen = CMSG_LEN(sizeof(TPayload));
  msg.msg_flags = 0;

  // Receive message.
  *fd = nullopt;
  for (;;) {
    auto rv = ::recvmsg(fd_, &msg, 0);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      return TP_CREATE_ERROR(SystemError, "recvmsg", errno);
    }
    if (rv != iov[0].iov_len) {
      return TP_CREATE_ERROR(ShortReadError, iov[0].iov_len, rv);
    }
    break;
  }

  *fd = Fd(*reinterpret_cast<TPayload*>(CMSG_DATA(cmsg)));
  return Error::kSuccess;
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
