/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/xth/context_impl.h>

#include <unistd.h>

#include <cstring>
#include <functional>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <tensorpipe/channel/xth/channel_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace xth {

std::shared_ptr<ContextImpl> ContextImpl::create() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";
  auto nsID = getLinuxNamespaceId(LinuxNamespace::kPid);
  if (!nsID.has_value()) {
    TP_VLOG(5)
        << "XTH channel is not viable because it couldn't determine the PID namespace ID";
    return nullptr;
  }
  oss << bootID.value() << "_" << nsID.value() << "_" << ::getpid();
  const std::string domainDescriptor = oss.str();

  std::unordered_map<Device, std::string> deviceDescriptors = {
      {Device{kCpuDeviceType, 0}, domainDescriptor}};
  return std::make_shared<ContextImpl>(std::move(deviceDescriptors));
}

ContextImpl::ContextImpl(
    std::unordered_map<Device, std::string> deviceDescriptors)
    : ContextImplBoilerplate<ContextImpl, ChannelImpl>(
          std::move(deviceDescriptors)),
      requests_(std::numeric_limits<int>::max()) {
  thread_ = std::thread(&ContextImpl::handleCopyRequests, this);
}

std::shared_ptr<Channel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(
      std::move(connections[0]), std::move(connections[1]));
}

size_t ContextImpl::numConnectionsNeeded() const {
  return 2;
}

void ContextImpl::handleErrorImpl() {
  requests_.push(nullopt);
}

void ContextImpl::joinImpl() {
  thread_.join();
  // TP_DCHECK(requests_.empty());
}

bool ContextImpl::inLoop() const {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

void ContextImpl::requestCopy(
    void* remotePtr,
    void* localPtr,
    size_t length,
    std::function<void(const Error&)> fn) {
  uint64_t requestId = nextRequestId_++;
  TP_VLOG(4) << "Channel context " << id_ << " received a copy request (#"
             << requestId << ")";

  fn = [this, requestId, fn{std::move(fn)}](const Error& error) {
    TP_VLOG(4) << "Channel context " << id_
               << " is calling a copy request callback (#" << requestId << ")";
    fn(error);
    TP_VLOG(4) << "Channel context " << id_
               << " done calling a copy request callback (#" << requestId
               << ")";
  };

  requests_.push(CopyRequest{remotePtr, localPtr, length, std::move(fn)});
}

void ContextImpl::handleCopyRequests() {
  setThreadName("TP_XTH_loop");
  while (true) {
    auto maybeRequest = requests_.pop();
    if (!maybeRequest.has_value()) {
      break;
    }
    CopyRequest request = std::move(maybeRequest).value();

    // Don't even call memcpy on a length of 0 to avoid issues with the pointer
    // possibly being null.
    if (request.length > 0) {
      // Perform copy.
      std::memcpy(request.localPtr, request.remotePtr, request.length);
    }

    request.callback(Error::kSuccess);
  }
}

} // namespace xth
} // namespace channel
} // namespace tensorpipe
