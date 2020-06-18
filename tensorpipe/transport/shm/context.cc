/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/shm/context.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/registry.h>
#include <tensorpipe/transport/shm/connection.h>
#include <tensorpipe/transport/shm/listener.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

namespace {

std::shared_ptr<Context> makeShmContext() {
  return std::make_shared<Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, shm, makeShmContext);

} // namespace

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

  TToken addReaction(TFunction fn) override;

  void removeReaction(TToken token) override;

  std::tuple<int, int> reactorFds() override;

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

Context::Impl::TToken Context::Impl::addReaction(TFunction fn) {
  return reactor_.add(std::move(fn));
}

void Context::Impl::removeReaction(TToken token) {
  reactor_.remove(token);
}

std::tuple<int, int> Context::Impl::reactorFds() {
  return reactor_.fds();
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
