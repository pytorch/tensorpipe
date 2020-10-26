/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/context.h>

#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <climits>

#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/socket.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/ibv/connection.h>
#include <tensorpipe/transport/ibv/context_impl.h>
#include <tensorpipe/transport/ibv/error.h>
#include <tensorpipe/transport/ibv/listener.h>
#include <tensorpipe/transport/ibv/loop.h>
#include <tensorpipe/transport/ibv/sockaddr.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"ibv:"};

std::string generateDomainDescriptor() {
  // It would be very cool if we could somehow obtain an "identifier" for the
  // InfiniBand subnet that our device belongs to, but nothing of that sort
  // seems to be available. So instead we say that if the user is trying to
  // connect two processes which both have access to an InfiniBand device then
  // they must know what they are doing and probably must have set up things
  // properly.
  return kDomainDescriptorPrefix + "*";
}

struct InterfaceAddressesDeleter {
  void operator()(struct ifaddrs* ptr) {
    ::freeifaddrs(ptr);
  }
};

using InterfaceAddresses =
    std::unique_ptr<struct ifaddrs, InterfaceAddressesDeleter>;

std::tuple<Error, InterfaceAddresses> createInterfaceAddresses() {
  struct ifaddrs* ifaddrs;
  auto rv = ::getifaddrs(&ifaddrs);
  if (rv < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "getifaddrs", errno),
        InterfaceAddresses());
  }
  return std::make_tuple(Error::kSuccess, InterfaceAddresses(ifaddrs));
}

std::tuple<Error, std::string> getHostname() {
  std::array<char, HOST_NAME_MAX> hostname;
  auto rv = ::gethostname(hostname.data(), hostname.size());
  if (rv < 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(SystemError, "gethostname", errno), std::string());
  }
  return std::make_tuple(Error::kSuccess, std::string(hostname.data()));
}

struct AddressInfoDeleter {
  void operator()(struct addrinfo* ptr) {
    ::freeaddrinfo(ptr);
  }
};

using AddressInfo = std::unique_ptr<struct addrinfo, AddressInfoDeleter>;

std::tuple<Error, AddressInfo> createAddressInfo(std::string host) {
  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  struct addrinfo* result;
  auto rv = ::getaddrinfo(host.c_str(), nullptr, &hints, &result);
  if (rv != 0) {
    return std::make_tuple(
        TP_CREATE_ERROR(GetaddrinfoError, rv), AddressInfo());
  }
  return std::make_tuple(Error::kSuccess, AddressInfo(result));
}

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl();

  bool isViable() const;

  const std::string& domainDescriptor() const;

  std::shared_ptr<transport::Connection> connect(std::string addr);

  std::shared_ptr<transport::Listener> listen(std::string addr);

  std::tuple<Error, std::string> lookupAddrForIface(std::string iface);

  std::tuple<Error, std::string> lookupAddrForHostname();

  void setId(std::string id);

  ClosingEmitter& getClosingEmitter() override;

  bool inLoopThread() override;

  void deferToLoop(std::function<void()> fn) override;

  void runInLoop(std::function<void()> fn) override;

  void registerDescriptor(int fd, int events, std::shared_ptr<EventHandler> h)
      override;

  void unregisterDescriptor(int fd) override;

  Reactor& getReactor() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  Reactor reactor_;
  Loop loop_{this->reactor_};
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

std::shared_ptr<transport::Connection> Context::connect(std::string addr) {
  return impl_->connect(std::move(addr));
}

std::shared_ptr<transport::Connection> Context::Impl::connect(
    std::string addr) {
  std::string connectionId = id_ + ".c" + std::to_string(connectionCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening connection "
             << connectionId << " to address " << addr;
  return std::make_shared<Connection>(
      Connection::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr),
      std::move(connectionId));
}

std::shared_ptr<transport::Listener> Context::listen(std::string addr) {
  return impl_->listen(std::move(addr));
}

std::shared_ptr<transport::Listener> Context::Impl::listen(std::string addr) {
  std::string listenerId = id_ + ".l" + std::to_string(listenerCounter_++);
  TP_VLOG(7) << "Transport context " << id_ << " is opening listener "
             << listenerId << " on address " << addr;
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr),
      std::move(listenerId));
}

bool Context::isViable() const {
  return impl_->isViable();
}

bool Context::Impl::isViable() const {
  return reactor_.isViable();
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::tuple<Error, std::string> Context::lookupAddrForIface(std::string iface) {
  return impl_->lookupAddrForIface(std::move(iface));
}

std::tuple<Error, std::string> Context::Impl::lookupAddrForIface(
    std::string iface) {
  Error error;
  InterfaceAddresses addresses;
  std::tie(error, addresses) = createInterfaceAddresses();
  if (error) {
    return std::make_tuple(std::move(error), std::string());
  }

  struct ifaddrs* ifa;
  for (ifa = addresses.get(); ifa != nullptr; ifa = ifa->ifa_next) {
    // Skip entry if ifa_addr is NULL (see getifaddrs(3))
    if (ifa->ifa_addr == nullptr) {
      continue;
    }

    if (iface != ifa->ifa_name) {
      continue;
    }

    switch (ifa->ifa_addr->sa_family) {
      case AF_INET:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(ifa->ifa_addr, sizeof(struct sockaddr_in)).str());
      case AF_INET6:
        return std::make_tuple(
            Error::kSuccess,
            Sockaddr(ifa->ifa_addr, sizeof(struct sockaddr_in6)).str());
    }
  }

  return std::make_tuple(TP_CREATE_ERROR(NoAddrFoundError), std::string());
}

std::tuple<Error, std::string> Context::lookupAddrForHostname() {
  return impl_->lookupAddrForHostname();
}

std::tuple<Error, std::string> Context::Impl::lookupAddrForHostname() {
  Error error;
  std::string hostname;
  std::tie(error, hostname) = getHostname();
  if (error) {
    return std::make_tuple(std::move(error), std::string());
  }

  AddressInfo info;
  std::tie(error, info) = createAddressInfo(std::move(hostname));
  if (error) {
    return std::make_tuple(std::move(error), std::string());
  }

  Error firstError;
  for (struct addrinfo* rp = info.get(); rp != nullptr; rp = rp->ai_next) {
    TP_DCHECK(rp->ai_family == AF_INET || rp->ai_family == AF_INET6);
    TP_DCHECK_EQ(rp->ai_socktype, SOCK_STREAM);
    TP_DCHECK_EQ(rp->ai_protocol, IPPROTO_TCP);

    Sockaddr addr = Sockaddr(rp->ai_addr, rp->ai_addrlen);

    std::shared_ptr<Socket> socket;
    std::tie(error, socket) = Socket::createForFamily(rp->ai_family);

    if (!error) {
      error = socket->bind(addr);
    }

    if (error) {
      // Record the first binding error we encounter and return that in the end
      // if no working address is found, in order to help with debugging.
      if (!firstError) {
        firstError = error;
      }
      continue;
    }

    return std::make_tuple(Error::kSuccess, addr.str());
  }

  if (firstError) {
    return std::make_tuple(std::move(firstError), std::string());
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
  reactor_.setId(id_);
}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
};

bool Context::Impl::inLoopThread() {
  return reactor_.inReactorThread();
};

void Context::Impl::deferToLoop(std::function<void()> fn) {
  reactor_.deferToLoop(std::move(fn));
};

void Context::Impl::runInLoop(std::function<void()> fn) {
  reactor_.runInLoop(std::move(fn));
};

void Context::Impl::registerDescriptor(
    int fd,
    int events,
    std::shared_ptr<EventHandler> h) {
  loop_.registerDescriptor(fd, events, std::move(h));
}

void Context::Impl::unregisterDescriptor(int fd) {
  loop_.unregisterDescriptor(fd);
}

Reactor& Context::Impl::getReactor() {
  return reactor_;
}

} // namespace ibv
} // namespace transport
} // namespace tensorpipe
