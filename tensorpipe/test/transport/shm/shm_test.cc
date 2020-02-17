/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

#include <tensorpipe/test/transport/transport_test.h>

INSTANTIATE_TYPED_TEST_CASE_P(Shm, TransportTest, SHMTransportTestHelper);
