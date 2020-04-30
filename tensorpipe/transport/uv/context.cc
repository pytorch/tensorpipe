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
  return std::make_shared<Connection>(
      Connection::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr));
}

std::shared_ptr<transport::Listener> Context::listen(address_t addr) {
  return impl_->listen(std::move(addr));
}

std::shared_ptr<transport::Listener> Context::Impl::listen(address_t addr) {
  return std::make_shared<Listener>(
      Listener::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(addr));
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
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
