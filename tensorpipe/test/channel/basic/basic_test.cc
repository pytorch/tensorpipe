/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/channel/basic/basic_test.h>

BasicChannelTestHelper helper;

INSTANTIATE_TEST_CASE_P(Basic, ChannelTest, ::testing::Values(&helper));
