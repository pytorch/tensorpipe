/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/benchmark/registry.h>
#include <tensorpipe/channel/context.h>

TP_DECLARE_SHARED_REGISTRY(
    TensorpipeChannelRegistry,
    tensorpipe::channel::Context);

void validateChannelContext(
    std::shared_ptr<tensorpipe::channel::Context> context);
