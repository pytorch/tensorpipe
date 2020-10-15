/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/ibv/context.h>

#include <tensorpipe/common/ibv.h>
#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/ibv/connection.h>
#include <tensorpipe/transport/ibv/listener.h>
#include <tensorpipe/transport/ibv/loop.h>
#include <tensorpipe/transport/registry.h>

namespace tensorpipe {
namespace transport {
namespace ibv {

namespace {

std::shared_ptr<Context> makeIbvContext() {
  return std::make_shared<Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, ibv, makeIbvContext);

} // namespace

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

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl();

  const std::string& domainDescriptor() const;

  std::shared_ptr<transport::Connection> connect(address_t addr);

  std::shared_ptr<transport::Listener> listen(address_t addr);

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

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
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
