/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/dl.h>

namespace tensorpipe {

// Wrapper for libibverbs.

class EfaLib {
 public:
 private:
  explicit EfaLib(DynamicLibraryHandle dlhandle)
      : dlhandle_(std::move(dlhandle)) {}

  DynamicLibraryHandle dlhandle_;
 public:
  EfaLib() = default;

  static std::tuple<Error, EfaLib> create() {
    Error error;
    DynamicLibraryHandle dlhandle;
    // To keep things "neat" and contained, we open in "local" mode (as opposed
    // to global) so that the ibverbs symbols can only be resolved through this
    // handle and are not exposed (a.k.a., "leaded") to other shared objects.
    std::tie(error, dlhandle) =
        DynamicLibraryHandle::create("libfabric.so", RTLD_LOCAL | RTLD_LAZY);
    if (error) {
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
    return std::make_tuple(Error::kSuccess, std::move(lib));
  }

};


} // namespace tensorpipe
