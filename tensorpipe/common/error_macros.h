/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/common/error.h>

#define TP_CREATE_ERROR(typ, ...) (Error(std::make_shared<typ>(__VA_ARGS__)))
