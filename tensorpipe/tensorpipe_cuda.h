/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config_cuda.h>

// High-level API

#include <tensorpipe/common/cuda_buffer.h>

// Channels

#include <tensorpipe/channel/cuda_basic/factory.h>
#include <tensorpipe/channel/cuda_xth/factory.h>

#if TENSORPIPE_HAS_CUDA_GDR_CHANNEL
#include <tensorpipe/channel/cuda_gdr/factory.h>
#endif // TENSORPIPE_HAS_CUDA_GDR_CHANNEL

#if TENSORPIPE_HAS_CUDA_IPC_CHANNEL
#include <tensorpipe/channel/cuda_ipc/factory.h>
#endif // TENSORPIPE_HAS_CUDA_IPC_CHANNEL
