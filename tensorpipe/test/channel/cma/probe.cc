/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>

#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include <tensorpipe/channel/cma/factory.h>
#include <tensorpipe/common/defs.h>

namespace {}

int main(int argc, char* argv[]) {
  TP_THROW_ASSERT_IF(argc < 1);
  if (argc != 3) {
    TP_LOG_INFO() << "Usage: " << argv[0]
                  << " [rank] [path to a UNIX domain socket]";
    return 0;
  }

  TP_LOG_INFO() << "My PID is " << ::getpid();

  int rank = std::strtol(argv[1], nullptr, 10);

  int rv;
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  TP_THROW_SYSTEM_IF(fd < 0, errno);

  struct sockaddr_un socketAddr;
  std::memset(&socketAddr, 0, sizeof(struct sockaddr_un));
  socketAddr.sun_family = AF_UNIX;
  std::strcpy(socketAddr.sun_path, argv[2]);

  if (rank == 0) {
    rv = ::bind(
        fd,
        reinterpret_cast<struct sockaddr*>(&socketAddr),
        sizeof(struct sockaddr_un));
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    rv = ::listen(fd, 0);
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    struct sockaddr_storage peerAddr;
    socklen_t peerAddrlen = sizeof(struct sockaddr_storage);
    do {
      rv = ::accept(
          fd, reinterpret_cast<struct sockaddr*>(&peerAddr), &peerAddrlen);
      TP_THROW_SYSTEM_IF(rv < 0 && errno != EINTR, errno);
    } while (rv < 0);
    int otherFd = rv;
    rv = ::close(fd);
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    rv = ::unlink(argv[2]);
    TP_THROW_SYSTEM_IF(rv < 0, errno);
    fd = otherFd;
  } else {
    do {
      rv = ::connect(
          fd,
          reinterpret_cast<struct sockaddr*>(&socketAddr),
          sizeof(struct sockaddr_un));
      TP_THROW_SYSTEM_IF(rv < 0 && errno != EINTR, errno);
    } while (rv < 0);
  }

  struct ucred peerCreds;
  std::memset(&peerCreds, 0, sizeof(struct ucred));
  socklen_t peerCredsLen = sizeof(struct ucred);
  rv = ::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &peerCreds, &peerCredsLen);

  pid_t peerPid = peerCreds.pid;

  TP_LOG_INFO() << "The peer's PID is " << peerPid;

  rv = ::prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  uint64_t outbox = 0x0123456789abcdef;
  void* outboxPtr = &outbox;
  TP_LOG_INFO() << "My outbox's address is 0x" << std::hex
                << reinterpret_cast<uintptr_t>(outboxPtr);
  rv = ::write(fd, &outboxPtr, sizeof(void*));
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  TP_THROW_ASSERT_IF(rv != sizeof(void*));
  void* peerOutboxPtr;
  rv = ::read(fd, &peerOutboxPtr, sizeof(void*));
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  TP_THROW_ASSERT_IF(rv != sizeof(void*));
  TP_LOG_INFO() << "The peer's inbox address is 0x" << std::hex
                << reinterpret_cast<uintptr_t>(peerOutboxPtr);

  uint64_t inbox;
  struct iovec localIov;
  std::memset(&localIov, 0, sizeof(struct iovec));
  localIov.iov_base = &inbox;
  localIov.iov_len = sizeof(uint64_t);
  struct iovec remoteIov;
  std::memset(&remoteIov, 0, sizeof(struct iovec));
  remoteIov.iov_base = peerOutboxPtr;
  remoteIov.iov_len = sizeof(uint64_t);

  ssize_t result = ::process_vm_readv(peerPid, &localIov, 1, &remoteIov, 1, 0);
  TP_LOG_INFO() << "Calling process_vm_readv returned " << result
                << ", errno is set to " << errno
                << " and my inbox now has value 0x" << std::hex << inbox;
  bool successful = false;
  if (result >= 0) {
    TP_THROW_ASSERT_IF(result != sizeof(uint64_t));
    TP_THROW_ASSERT_IF(inbox != 0x0123456789abcdef);
    successful = true;
  }

  uint8_t ack;
  rv = ::write(fd, &ack, sizeof(uint8_t));
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  TP_THROW_ASSERT_IF(rv != sizeof(uint8_t));
  rv = ::read(fd, &ack, sizeof(uint8_t));
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  TP_THROW_ASSERT_IF(rv != sizeof(uint8_t));

  rv = ::close(fd);
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  auto ctx = tensorpipe::channel::cma::create();
  TP_LOG_INFO() << "The CMA context's viability is: " << std::boolalpha
                << ctx->isViable();
  std::string descriptor;
  if (ctx->isViable()) {
    auto cpuDevice = tensorpipe::Device{tensorpipe::kCpuDeviceType, 0};
    auto deviceDescriptors = ctx->deviceDescriptors();
    auto iter = deviceDescriptors.find(cpuDevice);
    TP_DCHECK(iter != deviceDescriptors.end());
    descriptor = iter->second;
  }
  TP_LOG_INFO() << "Its descriptor is: " << descriptor;

  std::cout << "{\"syscall_success\": " << successful
            << ", \"viability\": " << ctx->isViable()
            << ", \"device_descriptor\": \"" << descriptor << "\"}"
            << std::endl;
}
