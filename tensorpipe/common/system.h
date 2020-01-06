#pragma once

#include <algorithm>
#include <chrono>
#include <fstream>
#include <set>
#include <sstream>
#include <string>

#include <x86intrin.h>

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
// Utility functions to access number of CPUs.
//
// HotPlug CPU is not supported. Facebook does not use it currently.
// XXX: Add HotPlug CPU support.
//

using CpuId = unsigned;

// Used for CPU_* macros and for fixed size per-CPU arrays.
// XXX: Expose this as a configuration parameter.
constexpr unsigned kMaxCpus = 512;
static_assert(kMaxCpus <= CPU_SETSIZE, "!");

struct CpuSet {
  // Bitset of CPUs.
  const cpu_set_t cpu_set;

  // Maximum Id for CPUs in set.
  const CpuId max_cpu_id;

  /// Maximum Id for online CPUs.
  const CpuId max_cpu_id_online;

  CpuSet(cpu_set_t cpu_set, CpuId max_cpu_id, CpuId max_cpu_id_online)
      : cpu_set{cpu_set},
        max_cpu_id{max_cpu_id},
        max_cpu_id_online{max_cpu_id_online} {}

  bool operator==(const CpuSet& other) const {
    return CPU_EQUAL(&cpu_set, &other.cpu_set) != 0 &&
        max_cpu_id == other.max_cpu_id &&
        max_cpu_id_online == other.max_cpu_id_online;
  }

  bool operator!=(const CpuSet& other) const {
    return !((*this) == other);
  }

  /// Initialize given cpu_set or to all online if nullopt.
  static CpuSet makeAllOnline();

  static CpuSet makeFromCpuSet(cpu_set_t cpu_set);

  template <class TCont>
  static CpuSet makeFromCont(const TCont& s) {
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    for (auto cpu : s) {
      if (cpu < 0) {
        TP_THROW_EINVAL() << "Invalid CPU ID: " << cpu;
      } else if (cpu >= kMaxCpus) {
        TP_THROW_EINVAL() << "Maximum CPU ID is " << kMaxCpus - 1
                          << " Got CPU ID: " << cpu
                          << ". Do you want to increase kMaxCpus?";
      }
      CPU_SET(static_cast<CpuId>(cpu), &cpus);
    }
    return makeFromCpuSet(cpus);
  }

  std::set<CpuId> asSet() const;

  /// The default behaviour for when user do not specify CpuSet.
  static CpuSet getOrDefault(optional<CpuSet> opt);

  bool hasCpu(CpuId cpu) const {
    return CPU_ISSET(static_cast<CpuId>(cpu), &cpu_set);
  }

  bool hasCpu(int cpu) const {
    if (cpu < 0) {
      return false;
    }
    if (cpu > max_cpu_id) {
      return false;
    }
    return CPU_ISSET(static_cast<CpuId>(cpu), &cpu_set);
  }

  CpuId cpu_set_next(CpuId cpu) const noexcept;

  CpuId cpu_first_set() const noexcept;

  size_t numCpus() const {
    int count = CPU_COUNT(&cpu_set);
    // Always non-negative. Safe to cast.
    return static_cast<size_t>(count);
  }
};

std::ostream& operator<<(std::ostream& os, const CpuSet& cpu_set);

/// Macro to iterate over all CPUs in CpuSet.
/// Similar functionality to Linux's kernel identically named macro.
#define for_each_cpu(cpu, cpu_set)                                            \
  for (CpuId(cpu) = (cpu_set).cpu_first_set(); (cpu) <= (cpu_set).max_cpu_id; \
       (cpu) = (cpu_set).cpu_set_next((cpu)))

CpuId cpu_set_next(CpuId cpu, const cpu_set_t& cpu_set) noexcept;

CpuId cpu_first_set(const cpu_set_t& cpu_set) noexcept;

/// Macro to iterate over all CPUs in <cpu_set>
/// Similar functionality to Linux's kernel identically named macro.
#define for_each_cpu_set_t(cpu, cpu_set)                        \
  for (CpuId(cpu) = cpu_first_set((cpu_set)); (cpu) < kMaxCpus; \
       (cpu) = cpu_set_next((cpu), (cpu_set)))

/// Get current CPU.
CpuId getCpu();

inline TimeStamp rdtscp(CpuId& cpu) {
  TimeStamp tstamp = __rdtscp(&cpu);
  // Lower 12 bits are the CPU, next 12 are socket.
  cpu = cpu & 0xFFF;
  return tstamp;
}

inline std::pair<TimeStamp, CpuId> rdtscp() {
  CpuId cpu_id;
  auto tstamp = rdtscp(cpu_id);
  return std::make_pair(tstamp, cpu_id);
}

cpu_set_t parseCpusRange(std::string cpu_range_str);

cpu_set_t parseCpusList(std::string cpus_str);

optional<std::string> getProcFsStr(const std::string& file_name, pid_t tid);

inline auto getProcFsComm(pid_t tid) {
  return getProcFsStr("comm", tid);
}

inline auto getProcFsCmdLine(pid_t tid) {
  return getProcFsStr("cmdline", tid);
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

} // namespace tensorpipe
