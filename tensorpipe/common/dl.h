/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <dlfcn.h>

#include <memory>
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

struct DynamicLibraryHandleDeleter {
  void operator()(void* ptr) {
    int res = ::dlclose(ptr);
    TP_THROW_ASSERT_IF(res != 0) << "dlclose() failed: " << ::dlerror();
  }
};

using DynamicLibraryHandle = std::unique_ptr<void, DynamicLibraryHandleDeleter>;

inline std::tuple<Error, DynamicLibraryHandle> createDynamicLibraryHandle(
    const char* filename,
    int flags) {
  void* ptr = ::dlopen(filename, flags);
  if (ptr == nullptr) {
    return std::make_tuple(
        TP_CREATE_ERROR(DlError, ::dlerror()), DynamicLibraryHandle());
  }
  return std::make_tuple(Error::kSuccess, DynamicLibraryHandle(ptr));
}

inline std::tuple<Error, void*> loadSymbol(
    DynamicLibraryHandle& handle,
    const char* name) {
  // Since dlsym doesn't return a specific value to signal errors (because NULL
  // is a valid return value), we need to detect errors by calling dlerror and
  // checking whether it returns a string or not (i.e., NULL). But in order to
  // do so, we must first reset the error, in case one was already recorded.
  ::dlerror();
  void* ptr = ::dlsym(handle.get(), name);
  char* err = ::dlerror();
  if (err != nullptr) {
    return std::make_tuple(TP_CREATE_ERROR(DlError, err), nullptr);
  }
  return std::make_tuple(Error::kSuccess, ptr);
}

} // namespace tensorpipe
