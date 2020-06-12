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

optional<std::string> getProcFsStr(const std::string& file_name, pid_t tid) {
  std::ostringstream oss;
  oss << "/proc/" << tid << "/" << file_name;
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

#ifdef __APPLE__
optional<std::string> _getBootID() {
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
optional<std::string> _getBootID() {
  std::ifstream f{"/proc/sys/kernel/random/boot_id"};
  if (!f.is_open()) {
    return nullopt;
  }
  std::string v;
  getline(f, v);
  f.close();
  return v;
}

#endif

optional<std::string> getBootID() {
  static optional<std::string> bootID = _getBootID();
  return bootID;
}

void setThreadName(std::string name) {
#ifdef __linux__
  pthread_setname_np(pthread_self(), name.c_str());
#endif
}

} // namespace tensorpipe
