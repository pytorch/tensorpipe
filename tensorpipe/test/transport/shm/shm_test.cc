/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/transport/shm/shm_test.h>

#include <sstream>

#include <sys/types.h>
#include <unistd.h>

std::string createUniqueShmAddr() {
  const ::testing::TestInfo* const test_info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::ostringstream ss;
  // Once we upgrade googletest, also use test_info->test_suite_name() here.
  ss << "tensorpipe_test_" << test_info->name() << "_" << getpid();
  return ss.str();
}

namespace {

SHMTransportTestHelper helper;

} // namespace

INSTANTIATE_TEST_CASE_P(Shm, TransportTest, ::testing::Values(&helper));
