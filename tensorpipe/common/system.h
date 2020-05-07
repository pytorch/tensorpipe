/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
TimeStamp DurationToTimeStamp(TDuration d) {
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

// Set the name of the current thread, if possible. Use only for debugging.
void setThreadName(std::string name);

} // namespace tensorpipe
