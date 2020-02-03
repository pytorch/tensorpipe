/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/defs.h>

// Note: this file must only be included from source files!

#define TP_THROW_UV(err) TP_THROW(std::runtime_error)
#define TP_THROW_UV_IF(cond, err) \
  if (unlikely(cond))             \
  TP_THROW_UV(err) << TP_STRINGIFY(cond) << ": " << uv_strerror(err)
