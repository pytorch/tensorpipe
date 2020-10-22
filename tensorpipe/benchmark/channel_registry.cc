/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/channel_registry.h>

#include <tensorpipe/tensorpipe.h>

TP_DEFINE_SHARED_REGISTRY(
    TensorpipeChannelRegistry,
    tensorpipe::channel::CpuContext);

// BASIC

std::shared_ptr<tensorpipe::channel::CpuContext> makeBasicChannel() {
  return std::make_shared<tensorpipe::channel::basic::Context>();
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, basic, makeBasicChannel);

// CMA

#if TENSORPIPE_HAS_CMA_CHANNEL
std::shared_ptr<tensorpipe::channel::CpuContext> makeCmaChannel() {
  return std::make_shared<tensorpipe::channel::cma::Context>();
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, cma, makeCmaChannel);
#endif // TENSORPIPE_HAS_CMA_CHANNEL

// MPT

std::shared_ptr<tensorpipe::channel::CpuContext> makeMptChannel() {
  throw std::runtime_error("mtp channel requires arguments");
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, mpt, makeMptChannel);

// XTH

std::shared_ptr<tensorpipe::channel::CpuContext> makeXthChannel() {
  return std::make_shared<tensorpipe::channel::xth::Context>();
}

TP_REGISTER_CREATOR(TensorpipeChannelRegistry, xth, makeXthChannel);
