/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error.h>

#define TP_CREATE_ERROR(typ, ...)         \
  (Error(                                 \
      std::make_shared<typ>(__VA_ARGS__), \
      TP_TRIM_FILENAME(__FILE__),         \
      __LINE__))
