/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/registry.h>

TP_DEFINE_SHARED_REGISTRY(
    TensorpipeTransportRegistry,
    tensorpipe::transport::Context);
