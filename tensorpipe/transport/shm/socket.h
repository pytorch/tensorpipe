#pragma once

#include <sys/socket.h>

#include <chrono>
#include <memory>

#include <tensorpipe/transport/shm/fd.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Sockaddr final {
 public:
  static Sockaddr createAbstractUnixAddr(const std::string& name);

  inline const struct sockaddr* addr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  inline socklen_t addrlen() const {
    return addrlen_;
  }

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

 private:
  // Configure send or recv timeout.
  void configureTimeout(int opt, std::chrono::milliseconds timeout);
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
