/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <dlfcn.h>
#include <link.h>

#include <array>
#include <climits>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>
#include <tensorpipe/common/error_macros.h>

namespace tensorpipe {

class DlError final : public BaseError {
 public:
  explicit DlError(char* error) : error_(error) {}

  std::string what() const override {
    return error_;
  }

 private:
  std::string error_;
};

class DynamicLibraryHandle {
 public:
  DynamicLibraryHandle() = default;

  static std::tuple<Error, DynamicLibraryHandle> create(
      const char* filename,
      int flags) {
    void* ptr = ::dlopen(filename, flags);
    if (ptr == nullptr) {
      return std::make_tuple(
          TP_CREATE_ERROR(DlError, ::dlerror()), DynamicLibraryHandle());
    }
    return std::make_tuple(Error::kSuccess, DynamicLibraryHandle(ptr));
  }

  bool hasValue() const {
    return ptr_ != nullptr;
  }

  std::tuple<Error, void*> loadSymbol(const char* name) {
    // Since dlsym doesn't return a specific value to signal errors (because
    // NULL is a valid return value), we need to detect errors by calling
    // dlerror and checking whether it returns a string or not (i.e., NULL). But
    // in order to do so, we must first reset the error, in case one was already
    // recorded.
    ::dlerror();
    void* ptr = ::dlsym(ptr_.get(), name);
    char* err = ::dlerror();
    if (err != nullptr) {
      return std::make_tuple(TP_CREATE_ERROR(DlError, err), nullptr);
    }
    return std::make_tuple(Error::kSuccess, ptr);
  }

  std::tuple<Error, std::string> getFilename() {
    struct link_map* linkMap;
    int rv = ::dlinfo(ptr_.get(), RTLD_DI_LINKMAP, &linkMap);
    if (rv < 0) {
      return std::make_tuple(
          TP_CREATE_ERROR(DlError, ::dlerror()), std::string());
    }
    std::array<char, PATH_MAX> path;
    char* resolvedPath = ::realpath(linkMap->l_name, path.data());
    if (resolvedPath == nullptr) {
      return std::make_tuple(
          TP_CREATE_ERROR(SystemError, "realpath", errno), std::string());
    }
    TP_DCHECK(resolvedPath == path.data());
    return std::make_tuple(Error::kSuccess, std::string(path.data()));
  }

 private:
  struct Deleter {
    void operator()(void* ptr) {
      int res = ::dlclose(ptr);
      TP_THROW_ASSERT_IF(res != 0) << "dlclose() failed: " << ::dlerror();
    }
  };

  DynamicLibraryHandle(void* ptr) : ptr_(ptr, Deleter{}) {}

  std::unique_ptr<void, Deleter> ptr_;
};

} // namespace tensorpipe
