/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/context.h>

#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/transport/registry.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/error.h>
#include <tensorpipe/transport/uv/listener.h>
#include <tensorpipe/transport/uv/loop.h>
#include <tensorpipe/transport/uv/sockaddr.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

namespace {

std::shared_ptr<Context> makeUvContext() {
  return std::make_shared<Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, uv, makeUvContext);

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"uv:"};

std::string generateDomainDescriptor() {
  return kDomainDescriptorPrefix + "*";
}

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl();

  const std::string& domainDescriptor() const;

  std::shared_ptr<transport::Connection> connect(address_t addr);

  std::shared_ptr<transport::Listener> listen(address_t addr);

  std::tuple<Error, address_t> lookupAddrForIface(std::string iface);

  std::tuple<Error, address_t> lookupAddrForHostname();

  void setId(std::string id);

  ClosingEmitter& getClosingEmitter() override;

  bool inLoopThread() override;

  void deferToLoop(std::function<void()> fn) override;

  void runInLoop(std::function<void()> fn) override;

  std::shared_ptr<TCPHandle> createHandle() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  Loop loop_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  ClosingEmitter closingEmitter_;

  std::string domainDescriptor_;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the transport's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Sequence numbers for the listeners and connections created by this context,
  // used to create their identifiers based off this context's identifier. They
  // will only be used for logging and debugging.
  std::atomic<uint64_t> listenerCounter_{0};
  std::atomic<uint64_t> connectionCounter_{0};

  std::tuple<Error, address_t> lookupAddrForHostnameFromLoop_();
};

Context::Context() : impl_(std::make_shared<Impl>()) {}

Context::Impl::Impl() : domainDescriptor_(generateDomainDescriptor()) {}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  if (!closed_.exchange(true)) {
    TP_VLOG(7) << "Transport context " << id_ << " is closing";

    closingEmitter_.close();
    loop_.close();

    TP_VLOG(7) << "Transport context " << id_ << " done closing";
  }
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(7) << "Transport context " << id_ << " is joining";

    loop_.join();

    TP_VLOG(7) << "Transport context " << id_ << " done joining";
  }
}

Context::~Context() {
  join();
}

std::shared_ptr<transport::Connection> Context::connect(address_t addr) {
  return impl_->connect(std::move(addr));
}

std::shared_ptr<transport::Connection> Context::Impl::connect(address_t addr) {
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening connection "
             << connectionId << " to address " << addr;
  return std::make_shared<Connection>(
      Connection::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr),
      std::move(connectionId));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  return impl_->listen(std::move(addr));
}

std::shared_ptr<transport::Listener> Context::Impl::listen(address_t addr) {
  std::string listenerId = id_ + ".l" + std::to_string(listenerCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening listener "
             << listenerId << " on address " << addr;
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr),
      std::move(listenerId));
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::tuple<Error, address_t> Context::lookupAddrForIface(std::string iface) {
  return impl_->lookupAddrForIface(std::move(iface));
}

std::tuple<Error, address_t> Context::Impl::lookupAddrForIface(
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

std::tuple<Error, address_t> Context::lookupAddrForHostname() {
  return impl_->lookupAddrForHostname();
}

std::tuple<Error, address_t> Context::Impl::lookupAddrForHostname() {
  Error error;
  std::string addr;
  runInLoop([this, &error, &addr]() {
    std::tie(error, addr) = lookupAddrForHostnameFromLoop_();
  });
  return std::make_tuple(std::move(error), std::move(addr));
}

std::tuple<Error, address_t> Context::Impl::lookupAddrForHostnameFromLoop_() {
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

void Context::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Context::Impl::setId(std::string id) {
  TP_VLOG(7) << "Transport context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
};

bool Context::Impl::inLoopThread() {
  return loop_.inLoopThread();
};

void Context::Impl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

void Context::Impl::runInLoop(std::function<void()> fn) {
  loop_.runInLoop(std::move(fn));
};

std::shared_ptr<TCPHandle> Context::Impl::createHandle() {
  return TCPHandle::create(loop_);
};

} // namespace uv
} // namespace transport
} // namespace tensorpipe
