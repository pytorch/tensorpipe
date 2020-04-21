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

optional<std::string> getBootID() {
  std::ifstream f{"/proc/sys/kernel/random/boot_id"};
  if (!f.is_open()) {
    return nullopt;
  }
  std::string v;
  getline(f, v);
  f.close();
  return v;
}

void setThreadName(std::string name) {
#ifdef __linux__
  pthread_setname_np(pthread_self(), name.c_str());
#endif
}

} // namespace tensorpipe
