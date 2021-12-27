/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <system_error>

// Branch hint macros. C++20 will include them as part of language.
#define likely(x) __builtin_expect((x) ? 1 : 0, 1)
#define unlikely(x) __builtin_expect((x) ? 1 : 0, 0)

/// Auxiliar class to build exception, fill up it's what message and throw
/// in a single line. Usually uses as r-value so that destructor is called
/// at end of line that created it, throwing the desired exception.
/// (See TP_THROW).
namespace tensorpipe {
template <class TException>
class ExceptionThrower final {
 public:
  template <class... TArgs>
  ExceptionThrower(TArgs&&... nonWhat) {
    exBuilder_ = [&](const std::string& what) {
      return TException(std::move(nonWhat)..., what);
    };
  }

  // Throw exception on destructor, when l-value instance goes of scope
  // and stream has been written. Use noexcept(false) to inform the compiler
  // that it's ok to throw in destructor.
  ~ExceptionThrower() noexcept(false) {
    throw exBuilder_(oss_.str() + "\"");
  }

  std::ostream& getStream() {
    return oss_;
  }

 protected:
  std::function<TException(const std::string&)> exBuilder_;
  std::ostringstream oss_;
};
} // namespace tensorpipe

//
// Macros to throw commonly used exceptions.
//
#define TP_STRINGIFY(s) #s
#define TP_EXPAND_TO_STR(s) TP_STRINGIFY(s)

// Strip all leading components up to the *last* occurrence of "tensorpipe/".
// This removes all the system-specific prefixes added by the compiler.
#define TP_TRIM_FILENAME(s)                                         \
  [](const char* filename) -> const char* {                         \
    while (true) {                                                  \
      const char* match = std::strstr(filename + 1, "tensorpipe/"); \
      if (match == nullptr) {                                       \
        break;                                                      \
      }                                                             \
      filename = match;                                             \
    }                                                               \
    return filename;                                                \
  }(s)

#define TP_LOG_LOC \
  TP_TRIM_FILENAME(__FILE__) << ":" << TP_EXPAND_TO_STR(__LINE__)
#define TP_LOG_PREFFIX "In " << __func__ << " at " << TP_LOG_LOC

#define TP_THROW(ex_type, ...)                                     \
  ::tensorpipe::ExceptionThrower<ex_type>(__VA_ARGS__).getStream() \
      << TP_LOG_PREFFIX << " \""

#define TP_THROW_EINVAL() TP_THROW(std::invalid_argument)

#define TP_THROW_SYSTEM(err) \
  TP_THROW(std::system_error, err, std::system_category())
#define TP_THROW_SYSTEM_IF(cond, err) \
  if (unlikely(cond))                 \
  TP_THROW_SYSTEM(err)

#define TP_THROW_SYSTEM_CODE(err) TP_THROW(std::system_error, err)
#define TP_THROW_SYSTEM_CODE_IF(cond, err) \
  if (unlikely(cond))                      \
  TP_THROW_SYSTEM_CODE(err) << TP_STRINGIFY(cond)

#define TP_THROW_ASSERT() TP_THROW(std::runtime_error)
#define TP_THROW_ASSERT_IF(cond) \
  if (unlikely(cond))            \
  TP_THROW_ASSERT() << TP_STRINGIFY(cond)

// Conditional throwing exception
#define TP_THROW_IF_NULLPTR(ptr) \
  if (unlikely(ptr == nullptr))  \
  TP_THROW_EINVAL() << TP_STRINGIFY(ptr) << " has nullptr value"

// Safe-cast to std::error_code
namespace tensorpipe {
inline std::error_code toErrorCode(ssize_t e) {
  if (unlikely(e <= 0)) {
    TP_THROW_EINVAL() << "Error not a positive number. "
                      << "Is this value really an error?";
  } else if (unlikely(e > std::numeric_limits<int>::max())) {
    TP_THROW_EINVAL() << "Error out of range. Is this really an error?";
  }
  return {static_cast<int>(e), std::system_category()};
}
} // namespace tensorpipe

//
// Simple logging to stderr. This macros can be replaced if a more
// sophisticated logging is used in the future.
// Currently, tensorpipe is meant be used as shared library and to use
// exceptions for error handling, so the need for logging in
// the library is reduced.
//
namespace tensorpipe {
class LogEntry final {
 public:
  explicit LogEntry(char type) {
    oss_ << type;

    // In C++17 use std::timespec.
    struct timeval tv;
    // In C++17 use std::timespec_get.
    gettimeofday(&tv, nullptr);
    struct std::tm tm;
    // Need to use localtime_r as std::localtime may not be thread-safe.
    localtime_r(&tv.tv_sec, &tm);
    oss_ << std::setfill('0') << std::setw(2) << 1 + tm.tm_mon << std::setw(2)
         << tm.tm_mday << ' ' << std::setw(2) << tm.tm_hour << ':'
         << std::setw(2) << tm.tm_min << ':' << std::setw(2) << tm.tm_sec << '.'
         << std::setw(6) << tv.tv_usec;

    // The glog format uses the thread ID but it's painful to get (there is a
    // gettid syscall, but it's not exposed in glibc) so we use the PID instead.
    oss_ << ' ' << std::setfill(' ') << std::setw(5) << getpid();
  }

  ~LogEntry() noexcept {
    // Multiple threads or processes writing to the same log (e.g., stderr)
    // might lead to interleaved text and thus garbled output. It seems that a
    // single write syscall is "rather" atomic so instead of issuing a separate
    // write for the trailing newline we append it to the message and write them
    // together.
    oss_ << std::endl;
    std::cerr << oss_.str();
  }

  std::ostream& getStream() {
    return oss_;
  }

 protected:
  std::ostringstream oss_;
};
} // namespace tensorpipe

#define TP_LOG_DEBUG() \
  ::tensorpipe::LogEntry('V').getStream() << ' ' << TP_LOG_LOC << "] "
#define TP_LOG_INFO() \
  ::tensorpipe::LogEntry('I').getStream() << ' ' << TP_LOG_LOC << "] "
#define TP_LOG_WARNING() \
  ::tensorpipe::LogEntry('W').getStream() << ' ' << TP_LOG_LOC << "] "
#define TP_LOG_ERROR() \
  ::tensorpipe::LogEntry('E').getStream() << ' ' << TP_LOG_LOC << "] "

#define TP_LOG_DEBUG_IF(cond) \
  if (unlikely(cond))         \
  TP_LOG_DEBUG()
#define TP_LOG_INFO_IF(cond) \
  if (unlikely(cond))        \
  TP_LOG_INFO()
#define TP_LOG_WARNING_IF(cond) \
  if (unlikely(cond))           \
  TP_LOG_WARNING()
#define TP_LOG_ERROR_IF(cond) \
  if (unlikely(cond))         \
  TP_LOG_ERROR()

#define __TP_EXPAND_OPD(opd) TP_STRINGIFY(opd) << "(" << (opd) << ")"

//
// Debug checks.
// Note that non-debug checks are not provided because developers
// must handle all errors explicitly.
//

#define __TP_DCHECK(a)  \
  if (unlikely(!((a)))) \
  TP_THROW_ASSERT() << "Expected true for " << __TP_EXPAND_OPD(a)

#define __TP_DCHECK_CMP(a, b, op)                        \
  if (unlikely(!((a)op(b))))                             \
  TP_THROW_ASSERT() << "Expected " << __TP_EXPAND_OPD(a) \
                    << " " TP_STRINGIFY(op) << " " << __TP_EXPAND_OPD(b)

// Expand macro only in debug mode.
#ifdef NDEBUG

#define _TP_DLOG() \
  while (false)    \
  TP_LOG_DEBUG()

#define _TP_DCHECK(a) \
  while (false)       \
  __TP_DCHECK(a)

#define _TP_DCHECK_CMP(a, b, op) \
  while (false)                  \
  __TP_DCHECK_CMP(a, b, op)

#else

#define _TP_DLOG() TP_LOG_DEBUG()

#define _TP_DCHECK(a) __TP_DCHECK(a)

#define _TP_DCHECK_CMP(a, b, op) __TP_DCHECK_CMP(a, b, op)

#endif

// Public API for debug logging.
#define TP_DLOG() _TP_DLOG()

// Public API for debug checks.
#define TP_DCHECK(a) _TP_DCHECK(a)
#define TP_DCHECK_EQ(a, b) _TP_DCHECK_CMP(a, b, ==)
#define TP_DCHECK_NE(a, b) _TP_DCHECK_CMP(a, b, !=)
#define TP_DCHECK_LT(a, b) _TP_DCHECK_CMP(a, b, <)
#define TP_DCHECK_LE(a, b) _TP_DCHECK_CMP(a, b, <=)
#define TP_DCHECK_GT(a, b) _TP_DCHECK_CMP(a, b, >)
#define TP_DCHECK_GE(a, b) _TP_DCHECK_CMP(a, b, >=)

//
// Verbose logging.
// Some logging is helpful to diagnose tricky production issues but is too
// verbose to keep on all the time. It also should not be controlled by the
// debug flags, as we want to allow it to be enabled in production builds.
//

// The level of each TP_VLOG call should reflect where the object issuing it is
// located in the stack , and whether it's a call that involves handling
// requests from objects higher up, or issuing requests to objects lower down.
// This brings us to the following classification:
// - level 1 is for requests that core classes receive from the user
// - level 2 is for generic core classes stuff
// - level 3 is for requests that core classes issue to channels/transports
// - level 4 is for requests that channels receive from core classes
// - level 5 is for generic channels stuff
// - level 6 is for requests that channels issue to transports
// - level 7 is for requests that transports receive from core classes/channels
// - level 8 is for generic transports stuff
// - level 9 is for how transports deal with system resources

namespace tensorpipe {
inline unsigned long getVerbosityLevelInternal() {
  char* levelStr = std::getenv("TP_VERBOSE_LOGGING");
  if (levelStr == nullptr) {
    return 0;
  }
  return std::strtoul(levelStr, /*str_end=*/nullptr, /*base=*/10);
}

inline unsigned long getVerbosityLevel() {
  static unsigned long level = getVerbosityLevelInternal();
  return level;
}
} // namespace tensorpipe

#define TP_VLOG(level) TP_LOG_DEBUG_IF(level <= getVerbosityLevel())

//
// Argument checks
//
#define TP_ARG_CHECK(a) \
  if (unlikely(!((a)))) \
  TP_THROW_EINVAL() << "Expected argument to be true: " << __TP_EXPAND_OPD(a)

#define _TP_ARG_CMP(a, b, op)                                     \
  if (unlikely(!((a)op(b))))                                      \
  TP_THROW_EINVAL() << "Expected argument " << __TP_EXPAND_OPD(a) \
                    << " " TP_STRINGIFY(_op_) << " " << __TP_EXPAND_OPD(b)

#define TP_ARG_CHECK_EQ(a, b) _TP_ARG_CMP(a, b, ==)
#define TP_ARG_CHECK_NE(a, b) _TP_ARG_CMP(a, b, !=)
#define TP_ARG_CHECK_LT(a, b) _TP_ARG_CMP(a, b, <)
#define TP_ARG_CHECK_LE(a, b) _TP_ARG_CMP(a, b, <=)
#define TP_ARG_CHECK_GT(a, b) _TP_ARG_CMP(a, b, >)
#define TP_ARG_CHECK_GE(a, b) _TP_ARG_CMP(a, b, >=)

// Define DEXCEPT macro that is noexcept only in debug mode.
#ifdef NDEBUG
#define DEXCEPT noexcept(true)
#else
#define DEXCEPT noexcept(false)
#endif

#define TP_LOG_EXCEPTION(e)                         \
  TP_LOG_ERROR() << "Exception in " << __FUNCTION__ \
                 << " . Message: " << e.what()
