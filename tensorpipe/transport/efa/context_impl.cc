/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/efa/context_impl.h>
#include <tensorpipe/common/efa_lib.h>
#include <tensorpipe/common/efa.h>
#include <tensorpipe/transport/efa/connection_impl.h>
#include <tensorpipe/transport/efa/listener_impl.h>

namespace tensorpipe {
namespace transport {
namespace efa {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"efa:"};

std::string generateDomainDescriptor() {
  // It would be very cool if we could somehow obtain an "identifier" for the
  // InfiniBand subnet that our device belongs to, but nothing of that sort
  // seems to be available. So instead we say that if the user is trying to
  // connect two processes which both have access to an InfiniBand device then
  // they must know what they are doing and probably must have set up things
  // properly.
  return kDomainDescriptorPrefix + "*";
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  Error error;
  EfaLib efaLib;
  std::tie(error, efaLib) = EfaLib::create();
  if (error) {
    TP_VLOG(7)
        << "efa transport is not viable because libfabric couldn't be loaded: "
        << error.what();
    return nullptr;
  }

  // bool isEfaAvailable = FabricEndpoint::isEfaAvailable();
  if (!FabricEndpoint::isEfaAvailable()){
    TP_VLOG(7)
        << "libfabric cannot find efa provider.";
    return nullptr;
  }

  // efaDeviceList deviceList;
  // std::tie(error, deviceList) = efaDeviceList::create(efaLib);
  // if (error && error.isOfType<SystemError>() &&
  //     error.castToType<SystemError>()->errorCode() == ENOSYS) {
  //   TP_VLOG(7) << "efa transport is not viable because it couldn't get list of "
  //              << "InfiniBand devices because the kernel module isn't loaded";
  //   return nullptr;
  // }
  // TP_THROW_ASSERT_IF(error)
  //     << "Couldn't get list of InfiniBand devices: " << error.what();

  // if (deviceList.size() == 0) {
  //   TP_VLOG(7) << "efa transport is not viable because it couldn't find any "
  //              << "InfiniBand NICs";
  //   return nullptr;
  // }

  // return std::make_shared<ContextImpl>(
  //     std::move(efaLib), std::move(deviceList));
  return std::make_shared<ContextImpl>();
}


ContextImpl::ContextImpl()
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          generateDomainDescriptor()) {
          }

// ContextImpl::ContextImpl(efaLib efaLib, efaDeviceList deviceList)
//     : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
//           generateDomainDescriptor()),
//       reactor_(std::move(efaLib), std::move(deviceList)) {}

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

Reactor& ContextImpl::getReactor() {
  return reactor_;
}

} // namespace efa
} // namespace transport
} // namespace tensorpipe
