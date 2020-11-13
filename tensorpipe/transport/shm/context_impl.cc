/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

std::string generateDomainDescriptor() {
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  return kDomainDescriptorPrefix + bootID.value();
}

} // namespace

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          generateDomainDescriptor()) {}

void ContextImpl::closeImpl() {
  loop_.close();
  reactor_.close();
}

void ContextImpl::joinImpl() {
  loop_.join();
  reactor_.join();
}

bool ContextImpl::inLoop() {
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
