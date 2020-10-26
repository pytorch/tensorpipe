/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/benchmark/transport_registry.h>

#include <tensorpipe/tensorpipe.h>

TP_DEFINE_SHARED_REGISTRY(
    TensorpipeTransportRegistry,
    tensorpipe::transport::Context);

// IBV

#if TENSORPIPE_HAS_IBV_TRANSPORT
std::shared_ptr<tensorpipe::transport::Context> makeIbvContext() {
  return std::make_shared<tensorpipe::transport::ibv::Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, ibv, makeIbvContext);
#endif // TENSORPIPE_HAS_IBV_TRANSPORT

// SHM

#if TENSORPIPE_HAS_SHM_TRANSPORT
std::shared_ptr<tensorpipe::transport::Context> makeShmContext() {
  return std::make_shared<tensorpipe::transport::shm::Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, shm, makeShmContext);
#endif // TENSORPIPE_HAS_SHM_TRANSPORT

// UV

std::shared_ptr<tensorpipe::transport::Context> makeUvContext() {
  return std::make_shared<tensorpipe::transport::uv::Context>();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, uv, makeUvContext);
