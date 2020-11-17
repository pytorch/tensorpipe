/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/context.h>

#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/uv/connection_impl.h>
#include <tensorpipe/transport/uv/context_impl.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/listener_impl.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"uv:"};

std::string generateDomainDescriptor() {
  return kDomainDescriptorPrefix + "*";
}

} // namespace

Context::Context() : impl_(std::make_shared<ContextImpl>()) {}

// Explicitly define all methods of the context, which just forward to the impl.
// We cannot use an intermediate ContextBoilerplate class without forcing a
// recursive include of private headers into the public ones.

std::shared_ptr<transport::Connection> Context::connect(std::string addr) {
  return impl_->connect(std::move(addr));
}

std::shared_ptr<transport::Listener> Context::listen(std::string addr) {
  return impl_->listen(std::move(addr));
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

void Context::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Context::close() {
  impl_->close();
}

void Context::join() {
  impl_->join();
}

Context::~Context() {
  join();
}

std::tuple<Error, std::string> Context::lookupAddrForIface(std::string iface) {
  return impl_->lookupAddrForIface(std::move(iface));
}

std::tuple<Error, std::string> Context::lookupAddrForHostname() {
  return impl_->lookupAddrForHostname();
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          generateDomainDescriptor()) {}

void ContextImpl::closeImpl() {
  loop_.close();
}

void ContextImpl::joinImpl() {
  loop_.join();
}

std::tuple<Error, std::string> ContextImpl::lookupAddrForIface(
    std::string iface) {
  int rv;
  InterfaceAddresses addresses;
  int count;
  std::tie(rv, addresses, count) = getInterfaceAddresses();
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  for (auto i = 0; i < count; i++) {
    const uv_interface_address_t& interface = addresses[i];
    if (iface != interface.name) {
      continue;
    }

    const auto& address = interface.address;
    const struct sockaddr* sockaddr =
        reinterpret_cast<const struct sockaddr*>(&address);
    switch (sockaddr->sa_family) {
      case AF_INET:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(sockaddr, sizeof(address.address4)).str());
      case AF_INET6:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(sockaddr, sizeof(address.address6)).str());
    }
  }

  return std::make_tuple(TP_CREATE_ERROR(NoAddrFoundError), std::string());
}

std::tuple<Error, std::string> ContextImpl::lookupAddrForHostname() {
  Error error;
  std::string addr;
  runInLoop([this, &error, &addr]() {
    std::tie(error, addr) = lookupAddrForHostnameFromLoop();
  });
  return std::make_tuple(std::move(error), std::move(addr));
}

std::tuple<Error, std::string> ContextImpl::lookupAddrForHostnameFromLoop() {
  int rv;
  std::string hostname;
  std::tie(rv, hostname) = getHostname();
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  Addrinfo info;
  std::tie(rv, info) = getAddrinfoFromLoop(loop_, std::move(hostname));
  if (rv < 0) {
    return std::make_tuple(TP_CREATE_ERROR(UVError, rv), std::string());
  }

  Error error;
  for (struct addrinfo* rp = info.get(); rp != nullptr; rp = rp->ai_next) {
    TP_DCHECK(rp->ai_family == AF_INET || rp->ai_family == AF_INET6);
    TP_DCHECK_EQ(rp->ai_socktype, SOCK_STREAM);
    TP_DCHECK_EQ(rp->ai_protocol, IPPROTO_TCP);

    Sockaddr addr = Sockaddr(rp->ai_addr, rp->ai_addrlen);

    std::shared_ptr<TCPHandle> handle = createHandle();
    handle->initFromLoop();
    rv = handle->bindFromLoop(addr);
    handle->closeFromLoop();

    if (rv < 0) {
      // Record the first binding error we encounter and return that in the end
      // if no working address is found, in order to help with debugging.
      if (!error) {
        error = TP_CREATE_ERROR(UVError, rv);
      }
      continue;
    }

    return std::make_tuple(Error::kSuccess, addr.str());
  }

  if (error) {
    return std::make_tuple(std::move(error), std::string());
  } else {
    return std::make_tuple(TP_CREATE_ERROR(NoAddrFoundError), std::string());
  }
}

bool ContextImpl::inLoop() {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

std::shared_ptr<TCPHandle> ContextImpl::createHandle() {
  return TCPHandle::create(loop_);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
