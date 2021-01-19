/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/common/system.h>

#ifdef __linux__
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

#endif

void setThreadName(std::string name) {
#ifdef __linux__
  pthread_setname_np(pthread_self(), name.c_str());
#endif
}

} // namespace tensorpipe
