/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/ibv/ibv_test.h>

namespace {

IbvTransportTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Ibv, TransportTest, ::testing::Values(&helper));
