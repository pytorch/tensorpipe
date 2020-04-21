/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <sys/types.h>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <system_error>

// Branch hint macros. C++20 will include them as part of language.
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/// Auxiliar class to build exception, fill up it's what message and throw
/// in a single line. Usually uses as r-value so that destructor is called
/// at end of line that created it, throwing the desired exception.
/// (See TP_THROW).
template <class TException>
class ExceptionThrower final {
 public:
  template <class... TArgs>
  ExceptionThrower(TArgs&&... non_what) {
    ex_builder_ = [&](const std::string& what) {
      return TException(std::move(non_what)..., what);
    };
  }

  // Throw exception on destructor, when l-value instance goes of scope
  // and stream has been written. Use noexcept(false) to inform the compiler
  // that it's ok to throw in destructor.
  ~ExceptionThrower() noexcept(false) {
    throw ex_builder_(oss_.str() + "\"");
  }

  std::ostream& getStream() {
    return oss_;
  }

 protected:
  std::function<TException(const std::string&)> ex_builder_;
  std::ostringstream oss_;
};

//
// Macros to throw commonly used exceptions.
//
#define TP_STRINGIFY(s) #s
#define TP_EXPAND_TO_STR(s) TP_STRINGIFY(s)

#define TP_LOG_LOC __FILE__ ":" TP_EXPAND_TO_STR(__LINE__)
#define TP_LOG_PREFFIX "In " << __func__ << " at " TP_LOG_LOC

#define TP_THROW(ex_type, ...) \
  ExceptionThrower<ex_type>(__VA_ARGS__).getStream() << TP_LOG_PREFFIX << " \""

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
inline std::error_code toErrorCode(ssize_t e) {
  if (unlikely(e <= 0)) {
    TP_THROW_EINVAL() << "Error not a positive number. "
                      << "Is this value really an error?";
  } else if (unlikely(e > std::numeric_limits<int>::max())) {
    TP_THROW_EINVAL() << "Error out of range. Is this really an error?";
  }
  return {static_cast<int>(e), std::system_category()};
}

//
// Simple logging to stderr. This macros can be replaced if a more
// sophisticated logging is used in the future.
// Currently, tensorpipe is meant be used as shared library and to use
// exceptions for error handling, so the need for logging in
// the library is reduced.
//
class LogEntry final {
 public:
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

#define TP_LOG_DEBUG() \
  LogEntry().getStream() << "[tensorpipe debug: " << TP_LOG_PREFFIX << "] "
#define TP_LOG_INFO() \
  LogEntry().getStream() << "[tensorpipe info: " << TP_LOG_PREFFIX << "] "
#define TP_LOG_WARNING() \
  LogEntry().getStream() << "[tensorpipe warning: " << TP_LOG_PREFFIX << "] "
#define TP_LOG_ERROR() \
  LogEntry().getStream() << "[tensorpipe error: " << TP_LOG_PREFFIX << "] "

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
// Eventually we may even want this to make this a runtime flag.
//

// Expand macro only when TP_VERBOSE_LOGGING is set.
#ifdef TP_VERBOSE_LOGGING

#define _TP_VLOG() TP_LOG_DEBUG()

#else

#define _TP_VLOG() \
  while (false)    \
  TP_LOG_DEBUG()

#endif

// Public API for verbose logging.
#define TP_VLOG() _TP_VLOG()

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
