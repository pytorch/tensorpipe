/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/context.h>

#include <algorithm>
#include <list>

#include <tensorpipe/channel/basic/channel.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/channel/registry.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {
namespace channel {
namespace basic {

namespace {

std::shared_ptr<Context> makeBasicChannel() {
  return std::make_shared<Context>();
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, basic, makeBasicChannel);

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl();

  const std::string& domainDescriptor() const;

  std::shared_ptr<channel::Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint);

  void setId(std::string id);

  ClosingEmitter& getClosingEmitter() override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  std::string domainDescriptor_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  ClosingEmitter closingEmitter_;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the channel's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Sequence numbers for the channels created by this context, used to create
  // their identifiers based off this context's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> channelCounter_{0};
};

Context::Context() : impl_(std::make_shared<Impl>()) {}

Context::Impl::Impl() : domainDescriptor_("any") {}

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

const std::string& Context::domainDescriptor() const {
  return impl_->domainDescriptor();
}

const std::string& Context::Impl::domainDescriptor() const {
  return domainDescriptor_;
}

std::shared_ptr<channel::Channel> Context::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint endpoint) {
  return impl_->createChannel(std::move(connection), endpoint);
}

std::shared_ptr<channel::Channel> Context::Impl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Channel::Endpoint /* unused */) {
  std::string channelId = id_ + ".c" + std::to_string(channelCounter_++);
  TP_VLOG(4) << "Channel context " << id_ << " is opening channel "
             << channelId;
  return std::make_shared<Channel>(
      Channel::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(connection),
      std::move(channelId));
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  if (!closed_.exchange(true)) {
    TP_VLOG(4) << "Channel context " << id_ << " is closing";

    closingEmitter_.close();

    TP_VLOG(4) << "Channel context " << id_ << " done closing";
  }
}

void Context::join() {
  impl_->join();
}

void Context::Impl::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(4) << "Channel context " << id_ << " is joining";

    // Nothing to do?

    TP_VLOG(4) << "Channel context " << id_ << " done joining";
  }
}

Context::~Context() {
  join();
}

void Context::setId(std::string id) {
  impl_->setId(std::move(id));
}

void Context::Impl::setId(std::string id) {
  TP_VLOG(4) << "Channel context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

} // namespace basic
} // namespace channel
} // namespace tensorpipe
