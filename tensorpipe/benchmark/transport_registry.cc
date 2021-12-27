/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
  return tensorpipe::transport::ibv::create();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, ibv, makeIbvContext);
#endif // TENSORPIPE_HAS_IBV_TRANSPORT

// SHM

#if TENSORPIPE_HAS_SHM_TRANSPORT
std::shared_ptr<tensorpipe::transport::Context> makeShmContext() {
  return tensorpipe::transport::shm::create();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, shm, makeShmContext);
#endif // TENSORPIPE_HAS_SHM_TRANSPORT

// UV

std::shared_ptr<tensorpipe::transport::Context> makeUvContext() {
  return tensorpipe::transport::uv::create();
}

TP_REGISTER_CREATOR(TensorpipeTransportRegistry, uv, makeUvContext);

void validateTransportContext(
    std::shared_ptr<tensorpipe::transport::Context> context) {
  if (!context) {
    auto keys = TensorpipeTransportRegistry().keys();
    std::cout
        << "The transport you passed in is not supported. The following transports are valid: ";
    for (const auto& key : keys) {
      std::cout << key << ", ";
    }
    std::cout << "\n";
    exit(EXIT_FAILURE);
  }
}
