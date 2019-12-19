#pragma once

#include <type_traits>

#include <unistd.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Fd {
 public:
  /* implicit */ Fd() {}

  explicit Fd(int fd) : fd_(fd) {}

  virtual ~Fd();

  // Disable copy constructor.
  Fd(const Fd&) = delete;

  // Disable copy assignment.
  Fd& operator=(const Fd&) = delete;

  // Custom move constructor.
  Fd(Fd&& other) {
    std::swap(fd_, other.fd_);
  }

  // Custom move assignment.
  Fd& operator=(Fd&& other) {
    std::swap(fd_, other.fd_);
    return *this;
  }

  // Return underlying file descriptor.
  inline int fd() const {
    return fd_;
  }

  // Release underlying file descriptor.
  int release() {
    auto fd = fd_;
    fd_ = -1;
    return fd;
  }

  // Proxy to read(2) with EINTR retry.
  ssize_t read(void* buf, size_t count);

  // Proxy to write(2) with EINTR retry.
  ssize_t write(const void* buf, size_t count);

  // Call read and throw if it doesn't complete.
  void readFull(void* buf, size_t count);

  // Call write and throw if it doesn't complete.
  void writeFull(const void* buf, size_t count);

  // Call `readFull` with trivially copyable type.
  template <typename T>
  T read() {
    T t;
    static_assert(std::is_trivially_copyable<T>::value, "!");
    readFull(&t, sizeof(T));
    return t;
  }

  // Call `writeFull` with trivially copyable type.
  template <typename T>
  void write(const T& t) {
    static_assert(std::is_trivially_copyable<T>::value, "!");
    writeFull(&t, sizeof(T));
  }

 protected:
  int fd_{-1};
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
