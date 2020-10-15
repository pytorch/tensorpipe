/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace {

// We should probably allow these to be user-configured. But, for now, we'll set
// them to the lowest value they can have, the rationale being that this way
// they will always be valid.
constexpr uint8_t kPortNum = 1;
constexpr uint8_t kGlobalIdentifierIndex = 0;

// FIXME Instead of hardcoding the next three values, we could use
// ibv_query_device to obtain max_cqe, max_qp_wr and max_srq_wr and deduce from
// them the maximum allowed values for these parameters.

// How many simultaneous receive requests to keep queued on the shared receive
// queue. Incoming RDMA writes and sends will consume one such request. The
// reactor loop will fill the SRQ back up to this value once some requests
// complete. So this number should just be large enough to accommodate all the
// requests that could finish between two reactor loop iterations. And, even if
// this number ends up being too low, the excess incoming requests will just
// retry, causing a performance penalty but not a failure.
constexpr uint32_t kNumPendingRecvReqs = 1024;

// How many RDMA write requests can be pending at the same time across all
// connections. We need to put a limit on them because they all use the same
// global completion queue which has a fixed capacity and if it overruns it will
// enter an unrecoverable error state. This value is also set as the capacity of
// the send queue of each queue pair.
constexpr uint32_t kNumPendingWriteReqs = 1024;

// How many send requests (used by the receiver to acknowledge the RDMA writes
// from the sender) can be pending at the same time across all connections.
constexpr uint32_t kNumPendingAckReqs = 1024;

// How many elements the completion queue should be able to hold. These elements
// will be either the completed receive requests of the SRQ, or the completed
// send requests from a connection's queue pair. We can bound the former value
// but not the latter, so we try to add some margin.
constexpr int kCompletionQueueSize =
    kNumPendingRecvReqs + kNumPendingWriteReqs + kNumPendingAckReqs;

// How many work completions to poll from the completion queue at each reactor
// iteration.
constexpr int kNumPolledWorkCompletions = 32;

} // namespace
