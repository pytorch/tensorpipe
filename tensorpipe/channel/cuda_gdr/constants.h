/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace tensorpipe {
namespace channel {
namespace cuda_gdr {

namespace {

// We should probably allow these to be user-configured. But, for now, we'll set
// them to the lowest value they can have, the rationale being that this way
// they will always be valid.
constexpr uint8_t kPortNum = 1;
constexpr uint8_t kGlobalIdentifierIndex = 0;

// FIXME Instead of hardcoding the next three values, we could use
// ibv_query_device to obtain max_cqe, max_qp_wr and max_srq_wr and deduce from
// them the maximum allowed values for these parameters.

constexpr uint32_t kNumRecvs = 1024;
constexpr uint32_t kNumSends = 1024;

// How many elements the completion queue should be able to hold. These elements
// will be either the completed receive requests of the SRQ, or the completed
// send requests from a connection's queue pair. We can bound the former value
// but not the latter, so we try to add some margin.
constexpr int kCompletionQueueSize = kNumRecvs + kNumSends;

// How many work completions to poll from the completion queue at each reactor
// iteration.
constexpr int kNumPolledWorkCompletions = 32;

} // namespace

} // namespace cuda_gdr
} // namespace channel
} // namespace tensorpipe
