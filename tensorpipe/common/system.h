/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

//
// TimeStamp is a 64 bit value representing
// a high-resolution clock. It is usually
// in nano-seconds or in TSC cycles.
//
using TimeStamp = uint64_t;
constexpr TimeStamp kInvalidTimeStamp = std::numeric_limits<TimeStamp>::max();

std::string tstampToStr(TimeStamp ts);

// std::chronos::duration to TSC.
template <class TDuration>
TimeStamp durationToTimeStamp(TDuration d) {
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
  if (ns < 0) {
    TP_THROW_EINVAL() << "Negative time durations are not valid";
  }
  return static_cast<TimeStamp>(ns);
}

//
// Useful math functions to work with CPU and binary integers
//

/// Is it a Power of 2?
constexpr bool isPow2(uint64_t n) noexcept {
  return n > 0 && !((n - 1) & n);
}

/// Smallest power of 2 larger or equal to <n>.
constexpr uint32_t nextPow2(uint32_t n) noexcept {
  --n;

  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;

  return n + 1;
}

/// Smallest power of 2 larger or equal to <n>
constexpr uint64_t nextPow2(uint64_t n) noexcept {
  --n;

  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;

  return n + 1;
}

/// Largest power of 2 less or equal to <n>
constexpr uint64_t maxPow2LessEqualThan(uint64_t n) noexcept {
  if (isPow2(n)) {
    return n;
  }
  return nextPow2(n) >> 1;
}

// Return contents of /proc/sys/kernel/random/boot_id.
optional<std::string> getBootID();

enum class LinuxNamespace {
  kIpc,
  kNet,
  kPid,
  kUser,
  // Add more entries as needed.
};

// Returns a string that uniquely identifies a namespace of a certain type.
// It is only valid within the same machine and for that fixed type.
optional<std::string> getLinuxNamespaceId(LinuxNamespace ns);

// Returns the names of the active Linux Security Modules, in the order in which
// they are employed by the kernel. The names could be arbitrary (as third-party
// LSMs could be in use) but contain values like "capability", "apparmor",
// "yama", "lockdown", ...
optional<std::vector<std::string>> getLinuxSecurityModules();

enum class YamaPtraceScope {
  kClassicPtracePermissions,
  kRestrictedPtrace,
  kAdminOnlyAttach,
  kNoAttach,
};

// YAMA is a Linux Security Module that specifically targets ptrace by locking
// down a process so it can only be targeted by its ancestors or by processes
// that it specifically selects. However YAMA can be disabled, or made even
// stricter. This function returns precisely what level YAMA is operating at.
optional<YamaPtraceScope> getYamaPtraceScope();

// Return a representation of the set of permitted capabilities of the process.
// We're talking about Linux kernel capabilities, see capabilities(7).
optional<std::string> getPermittedCapabilitiesID();

// Set the name of the current thread, if possible. Use only for debugging.
void setThreadName(std::string name);

} // namespace tensorpipe
