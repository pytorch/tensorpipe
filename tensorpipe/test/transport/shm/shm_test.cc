/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

namespace {

SHMTransportTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Shm, TransportTest, ::testing::Values(&helper));
