/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/basic/context_impl.h>

#include <functional>
#include <utility>

#include <tensorpipe/channel/basic/channel_impl.h>

namespace tensorpipe {
namespace channel {
namespace basic {

std::shared_ptr<ContextImpl> ContextImpl::create() {
  return std::make_shared<ContextImpl>();
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/true,
          /*domainDescriptor=*/"any") {}

std::shared_ptr<CpuChannel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(std::move(connections[0]));
}

void ContextImpl::closeImpl() {}

void ContextImpl::joinImpl() {}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

} // namespace basic
} // namespace channel
} // namespace tensorpipe
