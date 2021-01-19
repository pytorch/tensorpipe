/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/system.h>

#ifdef __linux__
#include <linux/capability.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef __APPLE__
#include <IOKit/IOKitLib.h>
#endif

#include <array>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>

#ifdef __linux__

// This is a libc wrapper for the Linux syscall.
// I'm not sure why we need to declare it ourselves, but that's what libcap
// does too, and I couldn't find any libc header in which it's declared.
// Direct use of the syscall is strongly discouraged, in favor of libcap (which
// has a more friendly API and better backwards-compatibility). However we
// really don't want to add a dependency, and moreover libcap introduces an
// artificial limitation that only allows us to query the capabilities that were
// defined by the kernel headers when libcap was built, meaning we might miss
// some (new) capabilities if the kernel was updated in the meantime.
extern "C" {
extern int capget(cap_user_header_t header, const cap_user_data_t data);
}

#endif

namespace tensorpipe {

namespace {

#ifdef __APPLE__
optional<std::string> getBootIDInternal() {
  std::array<char, 128> buf;

  // See https://developer.apple.com/documentation/iokit/iokitlib_h for IOKitLib
  // API documentation.
  io_registry_entry_t ioRegistryRoot =
      IORegistryEntryFromPath(kIOMasterPortDefault, "IOService:/");
  CFStringRef uuidCf = (CFStringRef)IORegistryEntryCreateCFProperty(
      ioRegistryRoot, CFSTR(kIOPlatformUUIDKey), kCFAllocatorDefault, 0);
  IOObjectRelease(ioRegistryRoot);
  CFStringGetCString(uuidCf, buf.data(), buf.size(), kCFStringEncodingMacRoman);
  CFRelease(uuidCf);

  return std::string(buf.data());
}

#elif defined(__linux__)
optional<std::string> getBootIDInternal() {
  std::ifstream f{"/proc/sys/kernel/random/boot_id"};
  if (!f.is_open()) {
    return nullopt;
  }
  std::string v;
  getline(f, v);
  f.close();
  return v;
}

// See namespaces(7).
std::string getPathForLinuxNamespace(LinuxNamespace ns) {
  std::ostringstream oss;
  oss << "/proc/self/ns/";
  switch (ns) {
    case LinuxNamespace::kIpc:
      oss << "ipc";
      break;
    case LinuxNamespace::kNet:
      oss << "net";
      break;
    case LinuxNamespace::kPid:
      oss << "pid";
      break;
    case LinuxNamespace::kUser:
      oss << "user";
      break;
    default:
      TP_THROW_ASSERT() << "Unknown namespace";
  }
  return oss.str();
}

#endif

} // namespace

std::string tstampToStr(TimeStamp ts) {
  if (ts == kInvalidTimeStamp) {
    return "NA";
  }
  // print timestaps in microseconds.
  constexpr TimeStamp kDiv = 1000u;
  std::stringstream ss;
  ss << std::setw(9) << std::setfill(' ') << ts / kDiv;
  ss << "." << std::setw(3) << std::setfill('0') << ts % kDiv << "us";
  return ss.str();
}

optional<std::string> getProcFsStr(const std::string& fileName, pid_t tid) {
  std::ostringstream oss;
  oss << "/proc/" << tid << "/" << fileName;
  std::ifstream f{oss.str()};
  if (!f.is_open()) {
    return nullopt;
  }
  std::string v;
  getline(f, v);
  f.close();
  return v;
}

std::string removeBlankSpaces(std::string s) {
  // Remove blanks.
  s.erase(
      std::remove_if(
          s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); }),
      s.end());
  return s;
}

optional<std::string> getBootID() {
  static optional<std::string> bootID = getBootIDInternal();
  return bootID;
}

#ifdef __linux__

// According to namespaces(7):
// > Each process has a /proc/[pid]/ns/ subdirectory containing one entry for
// > each namespace [...]. If two processes are in the same namespace, then the
// > device IDs and inode numbers of their /proc/[pid]/ns/xxx symbolic links
// > will be the same; an application can check this using the stat.st_dev and
// > stat.st_ino fields returned by stat(2).
optional<std::string> getLinuxNamespaceId(LinuxNamespace ns) {
  struct stat statInfo;
  int rv = ::stat(getPathForLinuxNamespace(ns).c_str(), &statInfo);
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  // These fields are of types dev_t and ino_t, which I couldn't find described
  // anywhere. They appear to be unsigned longs, but all we care about is that
  // they are integers, so let's check that.
  static_assert(std::is_integral<decltype(statInfo.st_dev)>::value, "");
  static_assert(std::is_integral<decltype(statInfo.st_ino)>::value, "");
  std::ostringstream oss;
  oss << std::hex << statInfo.st_dev << '_' << statInfo.st_ino;
  return oss.str();
}

// According to https://www.kernel.org/doc/Documentation/security/LSM.txt:
// > A list of the active security modules can be found by reading
// > /sys/kernel/security/lsm. This is a comma separated list [...].
optional<std::vector<std::string>> getLinuxSecurityModules() {
  std::ifstream f{"/sys/kernel/security/lsm"};
  if (f.fail()) {
    return nullopt;
  }
  // We shouldn't have to worry about an entirely empty file, as according to
  // the doc "[this list] will always include the capability module".
  std::vector<std::string> res;
  while (!f.eof()) {
    std::string lsm;
    std::getline(f, lsm, ',');
    TP_THROW_ASSERT_IF(f.fail());
    res.push_back(std::move(lsm));
  }
  f.close();
  TP_THROW_ASSERT_IF(f.fail());
  return res;
}

// See ptrace(2) (the sections towards the end) and
// https://www.kernel.org/doc/Documentation/security/Yama.txt
optional<YamaPtraceScope> getYamaPtraceScope() {
  std::ifstream f{"/proc/sys/kernel/yama/ptrace_scope"};
  if (f.fail()) {
    return nullopt;
  }
  int scope;
  f >> scope;
  TP_THROW_ASSERT_IF(f.fail());
  f.close();
  TP_THROW_ASSERT_IF(f.fail());
  switch (scope) {
    case 0:
      return YamaPtraceScope::kClassicPtracePermissions;
    case 1:
      return YamaPtraceScope::kRestrictedPtrace;
    case 2:
      return YamaPtraceScope::kAdminOnlyAttach;
    case 3:
      return YamaPtraceScope::kNoAttach;
    default:
      TP_THROW_ASSERT() << "Unrecognized YAMA ptrace scope: " << scope;
      // Dummy return to make the compiler happy.
      return nullopt;
  }
}

optional<std::string> getPermittedCapabilitiesID() {
  std::remove_pointer<cap_user_header_t>::type header;
  std::array<std::remove_pointer<cap_user_data_t>::type, 2> data;

  // At the time of writing there are three versions of the syscall supported
  // by the kernel, and we're supposed to perform a "handshake" to agree on the
  // latest version supported both by us and by the kernel. However, this is
  // only needed if we want to support pre-2.6.26 kernels, which we don't. Hence
  // we'll fail if the kernel doesn't support the latest version (v3). On the
  // other hand there is no way to figure out if the kernel's version has
  // advanced past the one we support. This will occur once there will be more
  // than 64 capabilities, but given the current pace this shouldn't happen for
  // quite a while. Such a limitation probably comes from the capability system
  // being designed around querying for a specific capability (in which case a
  // program only needs to support the syscall version where that capability was
  // added); querying _all_ capabilities (as we do) is kinda out-of-scope.
  header.version = 0x20080522;
  header.pid = 0;

  int rv = ::capget(&header, data.data());
  TP_THROW_SYSTEM_IF(rv < 0, errno);

  // We'll create a bitmask of the capabilities, and then return its hex.
  uint64_t bitmask = static_cast<uint64_t>(data[0].permitted) |
      (static_cast<uint64_t>(data[1].permitted) << 32);
  std::ostringstream oss;
  oss << std::hex << bitmask;
  return oss.str();
}

#endif

void setThreadName(std::string name) {
#ifdef __linux__
  pthread_setname_np(pthread_self(), name.c_str());
#endif
}

} // namespace tensorpipe
