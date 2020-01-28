#pragma once

#include <sys/socket.h>

#include <chrono>
#include <cstring>
#include <memory>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/error_macros.h>
#include <tensorpipe/transport/shm/fd.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

void saveOneFdToArray(int& dst, const int& src) {
  dst = src;
}

void saveOneFdToArray(int& dst, const Fd& src) {
  dst = src.fd();
}

template <size_t... Idxs, typename... Fds>
void saveFdsToArray(
    int* array,
    std::index_sequence<Idxs...>,
    const Fds&... fds) {
  // This is a trick to do pack expansion of the function call.
  auto dummy = {(saveOneFdToArray(array[Idxs], fds), 0)...};
}

void loadOneFdFromArray(int& src, int& dst) {
  dst = src;
}

void loadOneFdFromArray(int& src, Fd& dst) {
  dst = Fd(src);
}

template <size_t... Idxs, typename... Fds>
void loadFdsFromArray(int* array, std::index_sequence<Idxs...>, Fds&... fds) {
  // This is a trick to do pack expansion of the function call.
  auto dummy = {(loadOneFdFromArray(array[Idxs], fds), 0)...};
}

} // namespace

template <typename... Fds>
Error sendFdsToSocket(int socketFd, const Fds&... fds) {
  using TPayload = int;

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_flags = 0;

  // Build dummy iov with a single NUL byte.
  char nul = 0;
  struct iovec iov;
  iov.iov_base = &nul;
  iov.iov_len = sizeof(nul);
  msg.msg_iov = &iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iovec);

  // Build control message.
  uint8_t buf[CMSG_SPACE(sizeof(TPayload) * sizeof...(Fds))];
  msg.msg_control = &buf;
  msg.msg_controllen = sizeof(buf);

  struct cmsghdr* cmsg;
  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(TPayload) * sizeof...(Fds));
  auto payload = reinterpret_cast<TPayload*>(CMSG_DATA(cmsg));
  saveFdsToArray(payload, std::index_sequence_for<Fds...>{}, fds...);

  // Send message.
  for (;;) {
    auto rv = ::sendmsg(socketFd, &msg, 0);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      return TP_CREATE_ERROR(SystemError, "sendmsg", errno);
    }
    if (rv != iov.iov_len) {
      return TP_CREATE_ERROR(ShortWriteError, iov.iov_len, rv);
    }
    break;
  }

  return Error::kSuccess;
}

template <typename... Fds>
Error recvFdsFromSocket(int socketFd, Fds&... fds) {
  using TPayload = int;

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_flags = 0;

  // Build dummy iov with a single NUL byte.
  struct iovec iov;
  char nul = 0;
  iov.iov_base = &nul;
  iov.iov_len = sizeof(nul);
  msg.msg_iov = &iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iovec);

  // Build control message.
  uint8_t buf[CMSG_SPACE(sizeof(TPayload) * sizeof...(Fds))];
  msg.msg_control = &buf;
  msg.msg_controllen = sizeof(buf);

  // Receive message.
  for (;;) {
    auto rv = ::recvmsg(socketFd, &msg, 0);
    if (rv == -1) {
      if (errno == EINTR) {
        continue;
      }
      return TP_CREATE_ERROR(SystemError, "recvmsg", errno);
    }
    if (rv != iov.iov_len) {
      return TP_CREATE_ERROR(ShortReadError, iov.iov_len, rv);
    }
    break;
  }

  // Read control message.
  struct cmsghdr* cmsg;
  cmsg = CMSG_FIRSTHDR(&msg);
  TP_DCHECK_NE(cmsg, static_cast<void*>(nullptr));
  TP_DCHECK_EQ(cmsg->cmsg_level, SOL_SOCKET);
  TP_DCHECK_EQ(cmsg->cmsg_type, SCM_RIGHTS);
  TP_DCHECK_EQ(cmsg->cmsg_len, CMSG_LEN(sizeof(TPayload) * sizeof...(Fds)));
  auto payload = reinterpret_cast<TPayload*>(CMSG_DATA(cmsg));
  loadFdsFromArray(payload, std::index_sequence_for<Fds...>{}, fds...);

  return Error::kSuccess;
}

class Sockaddr final {
 public:
  static Sockaddr createAbstractUnixAddr(const std::string& name);

  inline const struct sockaddr* addr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const {
    return addrlen_;
  }

  std::string str() const;

 private:
  explicit Sockaddr(struct sockaddr* addr, socklen_t addrlen);

  struct sockaddr_storage addr_;
  socklen_t addrlen_;
};

class Socket final : public Fd, public std::enable_shared_from_this<Socket> {
 public:
  static std::shared_ptr<Socket> createForFamily(sa_family_t ai_family);

  explicit Socket(int fd) : Fd(fd) {}

  // Configure if the socket is blocking or not.
  void block(bool on);

  // Configure recv timeout.
  void recvTimeout(std::chrono::milliseconds timeout);

  // Configure send timeout.
  void sendTimeout(std::chrono::milliseconds timeout);

  // Bind socket to address.
  void bind(const Sockaddr& addr);

  // Listen on socket.
  void listen(int backlog);

  // Accept new socket connecting to listening socket.
  std::shared_ptr<Socket> accept();

  // Connect to address.
  void connect(const Sockaddr& addr);

  // Send file descriptor.
  template <typename... Fds>
  Error sendFds(const Fds&... fds) {
    return sendFdsToSocket(fd_, fds...);
  }

  // Receive file descriptor.
  template <typename... Fds>
  Error recvFds(Fds&... fds) {
    return recvFdsFromSocket(fd_, fds...);
  }

 private:
  // Configure send or recv timeout.
  void configureTimeout(int opt, std::chrono::milliseconds timeout);
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
