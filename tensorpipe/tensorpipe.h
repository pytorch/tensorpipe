/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/config.h>

// High-level API

#include <tensorpipe/core/buffer.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/error.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/core/pipe.h>

// Transports

#include <tensorpipe/transport/context.h>
#include <tensorpipe/transport/error.h>

#include <tensorpipe/transport/uv/context.h>
#include <tensorpipe/transport/uv/error.h>

#if TENSORPIPE_HAS_SHM_TRANSPORT
#include <tensorpipe/transport/shm/context.h>
#endif // TENSORPIPE_HAS_SHM_TRANSPORT

// Channels

#include <tensorpipe/channel/cpu_context.h>
#if TENSORPIPE_SUPPORTS_CUDA
#include <tensorpipe/channel/cuda_context.h>
#endif // TENSORPIPE_SUPPORTS_CUDA

#include <tensorpipe/channel/error.h>

#include <tensorpipe/channel/basic/context.h>
#include <tensorpipe/channel/mpt/context.h>
#include <tensorpipe/channel/xth/context.h>

#if TENSORPIPE_HAS_CMA_CHANNEL
#include <tensorpipe/channel/cma/context.h>
#endif // TENSORPIPE_HAS_CMA_CHANNEL

#if TENSORPIPE_HAS_CUDA_IPC_CHANNEL
#include <tensorpipe/channel/cuda_ipc/context.h>
#endif // TENSORPIPE_HAS_CUDA_IPC_CHANNEL
