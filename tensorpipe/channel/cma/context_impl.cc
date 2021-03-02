/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/cma/context_impl.h>

#include <linux/prctl.h>
#include <sys/prctl.h>
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
#include <tensorpipe/common/strings.h>
#include <tensorpipe/common/system.h>

namespace tensorpipe {
namespace channel {
namespace cma {

namespace {

// Prepend descriptor with transport name so it's easy to
// disambiguate descriptors when debugging.
const std::string kDomainDescriptorPrefix{"cma:"};

// Old versions of Docker use a default seccomp-bpf rule that blocks some
// ptrace-related syscalls. To find this out, we attempt such a call against
// ourselves, which is always allowed (it shortcuts all checks, including LSMs),
// hence a failure can only come from a "filter" on the syscall.
bool isProcessVmReadvSyscallAllowed() {
  uint64_t someSourceValue = 0x0123456789abcdef;
  uint64_t someTargetValue = 0;
  struct iovec source {
    .iov_base = &someSourceValue, .iov_len = sizeof(someSourceValue)
  };
  struct iovec target {
    .iov_base = &someTargetValue, .iov_len = sizeof(someTargetValue)
  };
  ssize_t nread = ::process_vm_readv(::getpid(), &target, 1, &source, 1, 0);
  return nread == sizeof(uint64_t) && someTargetValue == someSourceValue;
}

// According to read(2):
// > On Linux, read() (and similar system calls) will transfer at most
// > 0x7ffff000 (2,147,479,552) bytes, returning the number of bytes actually
// > transferred. (This is true on both 32-bit and 64-bit systems.)
constexpr size_t kMaxBytesReadableAtOnce = 0x7ffff000;

Error performCopy(
    void* localPtr,
    void* remotePtr,
    size_t length,
    pid_t remotePid) {
  for (size_t offset = 0; offset < length; offset += kMaxBytesReadableAtOnce) {
    size_t chunkLength = std::min(length - offset, kMaxBytesReadableAtOnce);
    struct iovec local {
      .iov_base = reinterpret_cast<uint8_t*>(localPtr) + offset,
      .iov_len = chunkLength
    };
    struct iovec remote {
      .iov_base = reinterpret_cast<uint8_t*>(remotePtr) + offset,
      .iov_len = chunkLength
    };
    auto nread = ::process_vm_readv(remotePid, &local, 1, &remote, 1, 0);
    if (nread == -1) {
      return TP_CREATE_ERROR(SystemError, "process_vm_readv", errno);
    } else if (nread != chunkLength) {
      return TP_CREATE_ERROR(ShortReadError, chunkLength, nread);
    }
  }
  return Error::kSuccess;
}

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create() {
  int rv;
  std::ostringstream oss;
  oss << kDomainDescriptorPrefix;

  // This transport only works across processes on the same machine, and we
  // detect that by computing the boot ID.
  optional<std::string> bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID.has_value()) << "Unable to read boot_id";
  oss << bootID.value();

  // An endpoint can see the other through its PID if the latter is in a child
  // PID namespace of the former. Since the channel is bidirectional this must
  // be symmetric and thus the PID namespaces must be the same.
  optional<std::string> pidNsID = getLinuxNamespaceId(LinuxNamespace::kPid);
  TP_THROW_ASSERT_IF(!pidNsID.has_value()) << "Unable to read pid namespace ID";
  oss << '_' << pidNsID.value();

  // The ability to call process_vm_readv on a target is controlled by the
  // PTRACE_MODE_ATTACH_REALCREDS check (see process_vm_readv(2)). We'll go
  // through its checklist, step by step (which is found in ptrace(2)). We will
  // ignore the CAP_SYS_PTRACE conditions (i.e., we'll assume we don't have that
  // capability) because they are hard to check, and typically not needed.

  // We'll skip the check on whether the endpoints are two threads of the same
  // process (in which case ptrace is always allowed) because it's hard to fit
  // it in the descriptor and because we have some other more specialized
  // channels for that case.

  // The next step involves comparing user and group IDs. If the processes are
  // in user namespaces the kernel first maps these IDs back to the top-level
  // ("initial") ones and compares those. We can't do such mapping, thus we
  // compare the IDs as integers as we see them and thus for this to work
  // properly we require that the two endpoints are in the same user namespace.
  // This does not in fact constitute an extra restriction since the later
  // commoncap/capability LSM check will need to enforce this too.
  optional<std::string> userNsID = getLinuxNamespaceId(LinuxNamespace::kUser);
  TP_THROW_ASSERT_IF(!userNsID.has_value())
      << "Unable to read user namespace ID";
  oss << '_' << userNsID.value();

  // It is required that our *real* user ID matches the real, effective and
  // saved-set user IDs of the target. And the same must hold for group IDs.
  // As the channel is bidirectional, the reverse must also hold, which means
  // our real, effective and saved-set IDs must all be equal and must match the
  // other endpoint's ones.
  uid_t realUserId, effectiveUserId, savedSetUserId;
  gid_t realGroupId, effectiveGroupId, savedSetGroupId;
  rv = ::getresuid(&realUserId, &effectiveUserId, &savedSetUserId);
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  rv = ::getresgid(&realGroupId, &effectiveGroupId, &savedSetGroupId);
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  if (realUserId != effectiveUserId || realUserId != savedSetUserId ||
      realGroupId != effectiveGroupId || realGroupId != savedSetGroupId) {
    TP_VLOG(5) << "User IDs or group IDs aren't all equal. User IDs are "
               << realUserId << " (real), " << effectiveUserId
               << " (effective) and " << savedSetUserId
               << " (saved-set). Group IDs are " << realGroupId << " (real), "
               << effectiveGroupId << " (effective) and " << savedSetGroupId
               << " (saved-set).";
    return std::make_shared<ContextImpl>();
  }
  oss << '_' << realUserId << '_' << realGroupId;

  // The target must be dumpable. Which, due to symmetry, means we must be
  // dumpable too.
  rv = ::prctl(PR_GET_DUMPABLE, 0, 0, 0, 0);
  TP_THROW_SYSTEM_IF(rv < 0, errno);
  // SUID_DUMP_USER has a value of 1.
  if (rv != 1) {
    TP_VLOG(5) << "Process isn't dumpable";
    return std::make_shared<ContextImpl>();
  }

  // Next the Linux Security Modules (LSMs) kick in. Since users could register
  // third-party LSMs we'll need to draw a line in what we support. We have two
  // options with unsupported LSMs: play it safe and assume the LSM will reject
  // the check, or "trust" the user and make them responsible to deal with the
  // LSMs they added. We're leaning for the latter, as often some LSMs like
  // AppArmor or SELinux are enabled without actually restricting anything. For
  // now we'll support the LSMs that are found by default on common distros,
  // but we can include support for more of them if that becomes necessary.
  optional<std::vector<std::string>> lsms = getLinuxSecurityModules();
  bool yamaOptional = false;
  if (!lsms.has_value()) {
    // This could happen if /sys/kernel/security/lsm cannot be opened. Although
    // that file looks like it resides on sysfs, it's actually on the securityfs
    // VFS, which is sometimes not bind-mounted inside containers. In such cases
    // rather than failing hard we'll check a couple of reasonable LSMs.
    TP_VLOG(5) << "Couldn't detect the active Linux Security Modules";
    lsms.emplace();
    *lsms = {"capability", "yama"};
    // We don't know whether YAMA is really there, hence we'll remember to
    // tolerate any failures later on.
    yamaOptional = true;
  } else {
    TP_VLOG(5) << "Detected these Linux Security Modules: " << joinStrs(*lsms);
  }
  // FIXME Can we assume that the two endpoints will see the same list of LSMs,
  // or should we incorporate that into the domain descriptor?
  for (const std::string& lsm : lsms.value()) {
    if (lsm == "capability") {
      // We already checked that the endpoints are in the same user namespace.
      // We must check they have the same permitted capabilities in it.
      optional<std::string> caps = getPermittedCapabilitiesID();
      TP_THROW_ASSERT_IF(!caps.has_value())
          << "Unable to obtain permitted capabilities";
      oss << '_' << caps.value();
    } else if (lsm == "yama") {
      optional<YamaPtraceScope> yamaScope = getYamaPtraceScope();
      if (!yamaScope.has_value()) {
        TP_THROW_ASSERT_IF(!yamaOptional)
            << "Unable to retrieve YAMA ptrace scope";
        continue;
      }
      switch (yamaScope.value()) {
        case YamaPtraceScope::kClassicPtracePermissions:
          TP_VLOG(5) << "YAMA ptrace scope set to classic ptrace persmissions";
          break;
        case YamaPtraceScope::kRestrictedPtrace:
          TP_VLOG(5) << "YAMA ptrace scope set to restricted ptrace";
          // FIXME It's not really great to change a global property of the
          // process, especially a security-related one. An "excuse" for doing
          // so is that UCT does the same:
          // https://github.com/openucx/ucx/blob/4d9976b6b8f8faae609c078c72aad8e5b842c43f/src/uct/sm/scopy/cma/cma_md.c#L61
          rv = ::prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
          TP_THROW_SYSTEM_IF(rv < 0, errno);
          break;
        case YamaPtraceScope::kAdminOnlyAttach:
          TP_VLOG(5) << "YAMA ptrace scope set to admin-only attach";
          return std::make_shared<ContextImpl>();
        case YamaPtraceScope::kNoAttach:
          TP_VLOG(5) << "YAMA ptrace scope set to no attach";
          return std::make_shared<ContextImpl>();
        default:
          TP_THROW_ASSERT() << "Unknown YAMA ptrace scope";
      }
    }
  }

  // In addition to the ptrace check, in some cases (I'm looking at you Docker)
  // the process_vm_readv syscall is outright blocked by seccomp-bpf.
  if (!isProcessVmReadvSyscallAllowed()) {
    TP_VLOG(5) << "The process_vm_readv syscall appears to be blocked";
    return std::make_shared<ContextImpl>();
  }

  std::string domainDescriptor = oss.str();
  TP_VLOG(5) << "The domain descriptor for CMA is " << domainDescriptor;
  return std::make_shared<ContextImpl>(std::move(domainDescriptor));
}

ContextImpl::ContextImpl()
    : ContextImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/false,
          /*domainDescriptor=*/"") {}

ContextImpl::ContextImpl(std::string domainDescriptor)
    : ContextImplBoilerplate<CpuBuffer, ContextImpl, ChannelImpl>(
          /*isViable=*/true,
          std::move(domainDescriptor)) {
  thread_ = std::thread(&ContextImpl::handleCopyRequests, this);
}

std::shared_ptr<CpuChannel> ContextImpl::createChannel(
    std::vector<std::shared_ptr<transport::Connection>> connections,
    Endpoint /* unused */) {
  TP_DCHECK_EQ(numConnectionsNeeded(), connections.size());
  return createChannelInternal(std::move(connections[0]));
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

    request.callback(performCopy(
        request.localPtr,
        request.remotePtr,
        request.length,
        request.remotePid));
  }
}

} // namespace cma
} // namespace channel
} // namespace tensorpipe
