/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/context_impl.h>

#include <sys/uio.h>
#include <unistd.h>

#include <functional>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <tensorpipe/channel/cma/channel_impl.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cma {

namespace {

std::string generateDomainDescriptor() {
  std::ostringstream oss;
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID) << "Unable to read boot_id";

  // According to the man page of process_vm_readv and process_vm_writev,
  // permission to read from or write to another process is governed by a ptrace
  // access mode PTRACE_MODE_ATTACH_REALCREDS check. This consists in a series
  // of checks, some governed by the CAP_SYS_PTRACE capability, others by the
  // Linux Security Modules (LSMs), but the primary constraint is that the real,
  // effective, and saved-set user IDs of the target match the caller's real
  // user ID, and the same for group IDs. Since channels are bidirectional, we
  // end up needing these IDs to all be the same on both processes.

  // Combine boot ID, effective UID, and effective GID.
  oss << bootID.value();
  // FIXME As domain descriptors are just compared for equality, we only include
  // the effective IDs, but we should abide by the rules above and make sure
  // that they match the real and saved-set ones too.
  oss << "/" << geteuid();
  oss << "/" << getegid();
  return oss.str();
}

} // namespace

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          generateDomainDescriptor()),
      requests_(std::numeric_limits<int>::max()) {
  thread_ = std::thread(&ContextImpl::handleCopyRequests, this);
}

std::shared_ptr<CpuChannel> ContextImpl::createChannel(
    std::shared_ptr<transport::Connection> connection,
    Endpoint /* unused */) {
  return createChannelInternal(std::move(connection));
}

void ContextImpl::closeImpl() {
  requests_.push(nullopt);
}

void ContextImpl::joinImpl() {
  thread_.join();
  // TP_DCHECK(requests_.empty());
}

bool ContextImpl::inLoop() {
  return loop_.inLoop();
};

void ContextImpl::deferToLoop(std::function<void()> fn) {
  loop_.deferToLoop(std::move(fn));
};

void ContextImpl::requestCopy(
    pid_t remotePid,
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

  requests_.push(
      CopyRequest{remotePid, remotePtr, localPtr, length, std::move(fn)});
}

void ContextImpl::handleCopyRequests() {
  setThreadName("TP_CMA_loop");
  while (true) {
    auto maybeRequest = requests_.pop();
    if (!maybeRequest.has_value()) {
      break;
    }
    CopyRequest request = std::move(maybeRequest).value();

    // Perform copy.
    struct iovec local {
      .iov_base = request.localPtr, .iov_len = request.length
    };
    struct iovec remote {
      .iov_base = request.remotePtr, .iov_len = request.length
    };
    auto nread =
        ::process_vm_readv(request.remotePid, &local, 1, &remote, 1, 0);
    if (nread == -1) {
      request.callback(TP_CREATE_ERROR(SystemError, "cma", errno));
    } else if (nread != request.length) {
      request.callback(TP_CREATE_ERROR(ShortReadError, request.length, nread));
    } else {
      request.callback(Error::kSuccess);
    }
  }
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
