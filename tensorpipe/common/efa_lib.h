/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/dl.h>
#include <memory>

namespace tensorpipe {

#define TP_FORALL_FABRIC_SYMBOLS(_) \
  _(fi_freeinfo)                    \
  _(fi_dupinfo)                     \
  _(fi_fabric)                      \
  _(fi_strerror)                    \
  _(fi_getinfo)

// Wrapper for libfabric.

class EfaLib {
 public:
  using device = struct fi_info;

 private:
  explicit EfaLib(DynamicLibraryHandle dlhandle)
      : dlhandle_(std::move(dlhandle)) {}

  DynamicLibraryHandle dlhandle_;

  decltype(&fi_allocinfo) fi_allocptr = nullptr;

#define TP_DECLARE_FIELD(function_name) \
  decltype(&function_name) function_name##_ptr_ = nullptr;
  TP_FORALL_FABRIC_SYMBOLS(TP_DECLARE_FIELD)
#undef TP_DECLARE_FIELD

 public:
  EfaLib() = default;

#define TP_FORWARD_CALL(function_name)                           \
  template <typename... Args>                                    \
  auto function_name##_op(Args&&... args) {                      \
    return (*function_name##_ptr_)(std::forward<Args>(args)...); \
  }
  TP_FORALL_FABRIC_SYMBOLS(TP_FORWARD_CALL)
#undef TP_FORWARD_CALL

  static std::tuple<Error, EfaLib> create() {
    Error error;
    DynamicLibraryHandle dlhandle;
    // To keep things "neat" and contained, we open in "local" mode (as opposed
    // to global) so that the ibverbs symbols can only be resolved through this
    // handle and are not exposed (a.k.a., "leaded") to other shared objects.
    std::tie(error, dlhandle) =
        DynamicLibraryHandle::create("libfabric.so", RTLD_LOCAL | RTLD_LAZY);
    if (error) {
      TP_LOG_WARNING() << "Load so fail";
      return std::make_tuple(std::move(error), EfaLib());
    }
    // Log at level 9 as we can't know whether this will be used in a transport
    // or channel, thus err on the side of this being as low-level as possible
    // because we don't expect this to be of interest that often.
    TP_VLOG(9) << [&]() -> std::string {
      std::string filename;
      std::tie(error, filename) = dlhandle.getFilename();
      if (error) {
        return "Couldn't determine location of shared library libfabric.so: " +
            error.what();
      }
      return "Found shared library libfabric.so at " + filename;
    }();
    EfaLib lib(std::move(dlhandle));
#define TP_LOAD_SYMBOL(function_name)                                \
  {                                                                  \
    void* ptr;                                                       \
    std::tie(error, ptr) = lib.dlhandle_.loadSymbol(#function_name); \
    if (error) {                                                     \
      return std::make_tuple(std::move(error), EfaLib());            \
    }                                                                \
    TP_THROW_ASSERT_IF(ptr == nullptr);                              \
    lib.function_name##_ptr_ =                                       \
        reinterpret_cast<decltype(function_name##_ptr_)>(ptr);       \
  }
    TP_FORALL_FABRIC_SYMBOLS(TP_LOAD_SYMBOL)
#undef TP_LOAD_SYMBOL
    return std::make_tuple(Error::kSuccess, std::move(lib));
  }
};

} // namespace tensorpipe
