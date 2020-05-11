/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/uv/context.h>

#include <tensorpipe/transport/registry.h>
#include <tensorpipe/transport/uv/connection.h>
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

} // namespace

namespace {

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
  uint64_t listenerCounter_{0};
  uint64_t connectionCounter_{0};
};

Context::Context() : impl_(std::make_shared<Impl>()) {}

Context::Impl::Impl() : domainDescriptor_(generateDomainDescriptor()) {}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  if (!closed_.exchange(true)) {
    closingEmitter_.close();
    loop_.close();
  }
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  if (!joined_.exchange(true)) {
    loop_.join();
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
  TP_VLOG() << "Transport context " << id_ << " is opening connection "
            << connectionId;
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
  TP_VLOG() << "Transport context " << id_ << " is opening listener "
            << listenerId;
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
