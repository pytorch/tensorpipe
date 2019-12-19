#include <tensorpipe/transport/shm/fd.h>

#include <unistd.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Fd::~Fd() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

ssize_t Fd::read(void* buf, size_t count) {
  ssize_t rv = -1;
  for (;;) {
    rv = ::read(fd_, buf, count);
    if (rv == -1 && errno == EINTR) {
      continue;
    }
    break;
  }
  return rv;
}

// Proxy to write(2) with EINTR retry.
ssize_t Fd::write(const void* buf, size_t count) {
  ssize_t rv = -1;
  for (;;) {
    rv = ::write(fd_, buf, count);
    if (rv == -1 && errno == EINTR) {
      continue;
    }
    break;
  }
  return rv;
}

// Call read and throw if it doesn't complete.
void Fd::readFull(void* buf, size_t count) {
  auto rv = read(buf, count);
  if (rv == -1) {
    TP_THROW_SYSTEM(errno);
  }
  TP_THROW_ASSERT_IF(rv < count) << "Partial read!";
}

// Call write and throw if it doesn't complete.
void Fd::writeFull(const void* buf, size_t count) {
  auto rv = write(buf, count);
  if (rv == -1) {
    TP_THROW_SYSTEM(errno);
  }
  TP_THROW_ASSERT_IF(rv < count) << "Partial write!";
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
