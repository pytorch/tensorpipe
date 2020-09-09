/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/registry.h>

TP_DEFINE_SHARED_REGISTRY(
    TensorpipeChannelRegistry,
    tensorpipe::channel::Context<tensorpipe::CpuTensor>);

#if TENSORPIPE_HAS_CUDA
TP_DEFINE_SHARED_REGISTRY(
    TensorpipeCudaChannelRegistry,
    tensorpipe::channel::Context<tensorpipe::CudaTensor>);
#endif // TENSORPIPE_HAS_CUDA
