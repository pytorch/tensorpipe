/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/context.h>

#include <sys/capability.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <limits>
#include <list>
#include <mutex>

#include <tensorpipe/channel/cma/channel.h>
#include <tensorpipe/channel/error.h>
#include <tensorpipe/channel/helpers.h>
#include <tensorpipe/channel/registry.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
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
  // The channel context already checks the capabilities, the LSM and the
  // equality between real, effective and saved-set IDs (and marks itself as
  // non-viable if any check fails). So the only check left to do is to compare
  // the real IDs of the two endpoints.

  // Combine boot ID, real UID, and real GID.
  oss << bootID.value();
  oss << "/" << getuid();
  oss << "/" << getgid();
  return oss.str();
}

std::shared_ptr<Context> makeCmaChannel() {
  return std::make_shared<Context>();
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, cma, makeCmaChannel);

int getYamaPtraceScope() {
  int scope;
  std::ifstream f;
  f.open("/proc/sys/kernel/yama/ptrace_scope");
  if (f.fail()) {
    // Assume that failing to open the file means it doesn't exist, and that
    // thus Yama Linux Security Module isn't being used. The classic ptrace
    // permissions therefore apply, which are represented as a scope of zero by
    // Yama.
    return 0;
  }
  f >> scope;
  if (f.fail()) {
    // The file could be opened but we couldn't obtain its value. We deduce that
    // Yama is on but we don't know in which state. To play it safe we report it
    // as being in the strictest state (which will mean the channel won't be
    // viable).
    return 3;
  }
  f.close();
  if (f.fail()) {
    // Same as above.
    return 3;
  }
  return scope;
}

// The value to give to PR_GET_DUMPABLE to make a process dumpable.
// See man 2 prctl.
constexpr int SUID_DUMP_USER = 1;

bool checkPermissions() {
  // The process_vm_readv syscall requires the calling process to have the
  // PTRACE_MODE_ATTACH_REALCREDS permission w.r.t. the target process. We try
  // to simulate such a check the best we can in order to keep the channel
  // disabled when it would fail. Our check is split in two parts: one done by
  // each process in isolation (this function; which marks the entire context as
  // non-viable) and one done during the handshake (by matching the descriptors;
  // which will deactivate the channel for that one pipe). For best accuracy we
  // should perform the entire check during the handshake but the simplicity of
  // the descriptor matching prevents that. Thus we have this function to try to
  // cope with that.
  // As each channel must be bidirectional we must check that both endpoints
  // have that permission for each other. Yet, some checks concern the ability
  // of one process to trace another, and other checks affect the ability to be
  // traced. The interplay between them isn't trivial. So, to simplify, we can
  // assume that the two endpoints are identical and thus checks are symmetric.

  int rv;

  // Processes with the CAP_SYS_PTRACE capability skip all checks and can attach
  // to any other process. Check this first and short-cut in case.
  cap_t caps = cap_init();
  TP_THROW_SYSTEM_IF(caps == NULL, errno) << "Failed call to cap_init";
  cap_flag_value_t capValue;
  rv = cap_get_flag(caps, CAP_SYS_PTRACE, CAP_EFFECTIVE, &capValue);
  TP_THROW_SYSTEM_IF(rv != 0, errno) << "Failed call to cap_get_flag";
  rv = cap_free(caps);
  TP_THROW_SYSTEM_IF(rv != 0, errno) << "Failed call to cap_free";

  if (capValue == CAP_SET) {
    return true;
  }

  // The first check that is performed matches the attaching process's real UID
  // and GID against the target process's real, effective, and saved-set UID and
  // GID. When matching the descriptors we'll check whether the read IDs match
  // but we must preliminarly check that the real IDs match the effective and
  // saved-set ones.
  uid_t ruid, euid, suid;
  rv = getresuid(&ruid, &euid, &suid);
  TP_THROW_SYSTEM_IF(rv != 0, errno) << "Failed call to getresuid";
  if (ruid != euid || ruid != suid) {
    return false;
  }
  gid_t rgid, egid, sgid;
  rv = getresgid(&rgid, &egid, &sgid);
  TP_THROW_SYSTEM_IF(rv != 0, errno) << "Failed call to getresgid";
  if (rgid != egid || rgid != sgid) {
    return false;
  }

  // The second check makes sure that the target process is dumpable.
  rv = prctl(PR_SET_DUMPABLE, SUID_DUMP_USER);
  TP_THROW_SYSTEM_IF(rv != 0, errno)
      << "Failed call to prctl(PR_SET_DUMPABLE, SUID_DUMP_USER)";

  // The third check defers to the kernel's Linux Security Module. We can't
  // possibly support all of them. We'll focus on the Yama one, as it's a common
  // one (shipped by default on Ubuntu) and it gives some headaches.
  // TODO Support the commoncap LSM too.
  int yamaScope = getYamaPtraceScope();
  switch (yamaScope) {
    case 0:
      // The Yama LSM doesn't add any additional constraints in this case.
      break;
    case 1:
      // In this case a process can only trace its descendants and processes
      // that have explicitly declared they allow that first process to attach
      // to them or, as well, that they allow any process to do that. This is
      // exactly what we'll do here.
      rv = prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY);
      TP_THROW_SYSTEM_IF(rv != 0, errno)
          << "Failed call to prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY)";
      break;
    case 2:
      // Only processes with the CAP_SYS_PTRACE capability can attach to other
      // processes. We already checked for that, which means that if we arrived
      // here we don't have that capability.
      return false;
    case 3:
      // Attaching is always forbidden in this case.
      return false;
    default:
      TP_THROW_ASSERT() << "Yama ptrace scope out of range: " << yamaScope;
  }

  return true;
}

} // namespace

class Context::Impl : public Context::PrivateIface,
                      public std::enable_shared_from_this<Context::Impl> {
 public:
  Impl();

  bool isViable() const;

  const std::string& domainDescriptor() const;

  std::shared_ptr<channel::Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint);

  void setId(std::string id);

  ClosingEmitter& getClosingEmitter() override;

  using copy_request_callback_fn = std::function<void(const Error&)>;

  void requestCopy(
      pid_t remotePid,
      void* remotePtr,
      void* localPtr,
      size_t length,
      copy_request_callback_fn fn) override;

  void close();

  void join();

  ~Impl() override = default;

 private:
  struct CopyRequest {
    pid_t remotePid;
    void* remotePtr;
    void* localPtr;
    size_t length;
    copy_request_callback_fn callback;
  };

  bool isViable_;
  std::string domainDescriptor_;
  std::thread thread_;
  Queue<optional<CopyRequest>> requests_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  ClosingEmitter closingEmitter_;

  // This is atomic because it may be accessed from outside the loop.
  std::atomic<uint64_t> nextRequestId_{0};

  // An identifier for the context, composed of the identifier for the context,
  // combined with the channel's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  // Sequence numbers for the channels created by this context, used to create
  // their identifiers based off this context's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> channelCounter_{0};

  void handleCopyRequests_();
};

Context::Context() : impl_(std::make_shared<Context::Impl>()) {}

Context::Impl::Impl()
    : isViable_(checkPermissions()),
      domainDescriptor_(generateDomainDescriptor()),
      requests_(std::numeric_limits<int>::max()) {
  // FIXME Set the context in an error state if non-viable, and don't even
  // bother starting the thread as it will be joined right away.
  thread_ = std::thread(&Impl::handleCopyRequests_, this);
}

void Context::close() {
  impl_->close();
}

void Context::Impl::close() {
  if (!closed_.exchange(true)) {
    TP_VLOG(4) << "Channel context " << id_ << " is closing";

    closingEmitter_.close();
    requests_.push(nullopt);

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

    thread_.join();
    // TP_DCHECK(requests_.empty());

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

ClosingEmitter& Context::Impl::getClosingEmitter() {
  return closingEmitter_;
}

bool Context::isViable() const {
  return impl_->isViable();
}

bool Context::Impl::isViable() const {
  return checkPermissions();
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
  TP_THROW_ASSERT_IF(joined_);
  std::string channelId = id_ + ".c" + std::to_string(channelCounter_++);
  TP_VLOG(4) << "Channel context " << id_ << " is opening channel "
             << channelId;
  return std::make_shared<Channel>(
      Channel::ConstructorToken(),
      std::static_pointer_cast<PrivateIface>(shared_from_this()),
      std::move(connection),
      std::move(channelId));
}

void Context::Impl::requestCopy(
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

void Context::Impl::handleCopyRequests_() {
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
