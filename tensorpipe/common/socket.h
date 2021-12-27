/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/socket.h>

#include <chrono>
#include <cstring>
#include <memory>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/fd.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

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
    std::index_sequence<Idxs...> /*unused*/,
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
void loadFdsFromArray(
    int* array,
    std::index_sequence<Idxs...> /*unused*/,
    Fds&... fds) {
  // This is a trick to do pack expansion of the function call.
  auto dummy = {(loadOneFdFromArray(array[Idxs], fds), 0)...};
}

} // namespace

template <typename T, typename... Fds>
[[nodiscard]] Error sendToSocket(
    int socketFd,
    const T& t1,
    const T& t2,
    const Fds&... fds) {
  using TPayload = int;

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_flags = 0;

  // Build iov to write Ts.
  std::array<T, 2> tbuf = {t1, t2};
  struct iovec iov;
  iov.iov_base = tbuf.data();
  iov.iov_len = sizeof(tbuf);
  msg.msg_iov = &iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iovec);

  // Build control message.
  std::array<uint8_t, CMSG_SPACE(sizeof(TPayload) * sizeof...(Fds))> buf;
  msg.msg_control = buf.data();
  msg.msg_controllen = buf.size();

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
[[nodiscard]] Error sendFdsToSocket(int socketFd, const Fds&... fds) {
  char dummy = 0;
  return sendToSocket(socketFd, dummy, dummy, fds...);
}

template <typename T, typename... Fds>
[[nodiscard]] Error recvFromSocket(int socketFd, T& t1, T& t2, Fds&... fds) {
  using TPayload = int;

  // Build message.
  struct msghdr msg;
  msg.msg_name = nullptr;
  msg.msg_namelen = 0;
  msg.msg_flags = 0;

  // Build iov to read Ts.
  std::array<T, 2> tbuf;
  struct iovec iov;
  iov.iov_base = tbuf.data();
  iov.iov_len = sizeof(tbuf);
  msg.msg_iov = &iov;
  msg.msg_iovlen = sizeof(iov) / sizeof(iovec);

  // Build control message.
  std::array<uint8_t, CMSG_SPACE(sizeof(TPayload) * sizeof...(Fds))> buf;
  msg.msg_control = buf.data();
  msg.msg_controllen = buf.size();

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

  t1 = tbuf[0];
  t2 = tbuf[1];

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

template <typename... Fds>
[[nodiscard]] Error recvFdsFromSocket(int socketFd, Fds&... fds) {
  char dummy = 0;
  return recvFromSocket(socketFd, dummy, dummy, fds...);
}

class Sockaddr {
 public:
  virtual const struct sockaddr* addr() const = 0;

  virtual socklen_t addrlen() const = 0;

  virtual ~Sockaddr() = default;
};

class Socket final : public Fd {
 public:
  [[nodiscard]] static std::tuple<Error, Socket> createForFamily(
      sa_family_t aiFamily);

  Socket() = default;

  explicit Socket(int fd) : Fd(fd) {}

  // Configure if the socket is blocking or not.
  [[nodiscard]] Error block(bool on);

  // Set (or unset) the SO_REUSEADDR option on the socket.
  [[nodiscard]] Error reuseAddr(bool on);

  // Bind socket to address.
  [[nodiscard]] Error bind(const Sockaddr& addr);

  // Listen on socket.
  [[nodiscard]] Error listen(int backlog);

  // Accept new socket connecting to listening socket.
  [[nodiscard]] std::tuple<Error, Socket> accept();

  // Connect to address.
  [[nodiscard]] Error connect(const Sockaddr& addr);

  [[nodiscard]] std::tuple<Error, struct sockaddr_storage, socklen_t>
  getSockName() const;

  // Send file descriptor.
  template <typename... Fds>
  [[nodiscard]] Error sendFds(const Fds&... fds) {
    return sendFdsToSocket(fd_, fds...);
  }

  // Receive file descriptor.
  template <typename... Fds>
  [[nodiscard]] Error recvFds(Fds&... fds) {
    return recvFdsFromSocket(fd_, fds...);
  }

  // Send object and file descriptor.
  template <
      typename T,
      typename... Fds,
      typename std::enable_if<std::is_trivially_copyable<T>::value, bool>::
          type = false>
  [[nodiscard]] Error sendPayloadAndFds(
      const T& t1,
      const T& t2,
      const Fds&... fds) {
    return sendToSocket(fd_, t1, t2, fds...);
  }

  // Receive object and file descriptor.
  template <
      typename T,
      typename... Fds,
      typename std::enable_if<std::is_trivially_copyable<T>::value, bool>::
          type = false>
  [[nodiscard]] Error recvPayloadAndFds(T& t1, T& t2, Fds&... fds) {
    return recvFromSocket(fd_, t1, t2, fds...);
  }
};

} // namespace tensorpipe
