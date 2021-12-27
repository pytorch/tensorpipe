/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/context_impl.h>

#include <tensorpipe/common/epoll_loop.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/shm/connection_impl.h>
#include <tensorpipe/transport/shm/listener_impl.h>
#include <tensorpipe/transport/shm/reactor.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"shm:"};

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  std::ostringstream oss;
  oss << kDomainDescriptorPrefix;

  // This transport only works across processes on the same machine, and we
  // detect that by computing the boot ID.
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID.has_value()) << "Unable to read boot_id";
  oss << bootID.value();

  // This transport bootstraps a connection by opening a UNIX domain socket, for
  // which it uses an "abstract" address (i.e., just an identifier, which is not
  // materialized to a filesystem path). In order for the two endpoints to
  // access each other's address they must be in the same Linux kernel network
  // namespace (see network_namespaces(7)).
  auto nsID = getLinuxNamespaceId(LinuxNamespace::kNet);
  if (!nsID.has_value()) {
    TP_VLOG(8) << "Unable to read net namespace ID";
    return nullptr;
  }
  oss << '_' << nsID.value();

  // Over that UNIX domain socket, the two endpoints exchange file descriptors
  // to regions of shared memory. Some restrictions may be in place that prevent
  // allocating such regions, hence let's allocate one here to see if it works.
  Error error;
  ShmSegment segment;
  std::tie(error, segment) = ShmSegment::alloc(1024 * 1024);
  if (error) {
    TP_VLOG(8) << "Couldn't allocate shared memory segment: " << error.what();
    return nullptr;
  }

  // A separate problem is that /dev/shm may be sized too small for all the
  // memory we need to allocate. However, our memory usage is unbounded, as it
  // grows as we open more connections, hence we cannot check it in advance.

  std::string domainDescriptor = oss.str();
  TP_VLOG(8) << "The domain descriptor for SHM is " << domainDescriptor;
  return std::make_shared<ContextImpl>(std::move(domainDescriptor));
}

ContextImpl::ContextImpl(std::string domainDescriptor)
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          std::move(domainDescriptor)) {}

void ContextImpl::handleErrorImpl() {
  loop_.close();
  reactor_.close();
}

void ContextImpl::joinImpl() {
  loop_.join();
  reactor_.join();
}

bool ContextImpl::inLoop() const {
  return reactor_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  reactor_.deferToLoop(std::move(fn));
};

void ContextImpl::registerDescriptor(
    int fd,
    int events,
    std::shared_ptr<EpollLoop::EventHandler> h) {
  loop_.registerDescriptor(fd, events, std::move(h));
}

void ContextImpl::unregisterDescriptor(int fd) {
  loop_.unregisterDescriptor(fd);
}

ContextImpl::TToken ContextImpl::addReaction(TFunction fn) {
  return reactor_.add(std::move(fn));
}

void ContextImpl::removeReaction(TToken token) {
  reactor_.remove(token);
}

std::tuple<int, int> ContextImpl::reactorFds() {
  return reactor_.fds();
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
