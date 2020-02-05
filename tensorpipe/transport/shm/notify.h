/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>

namespace tensorpipe {
namespace transport {
namespace shm {

// Extra data stored in ringbuffer header.
struct NotificationRingBufferExtraData {
  // Nothing yet.
};

using TNotificationRingBuffer =
    util::ringbuffer::RingBuffer<NotificationRingBufferExtraData>;
using TNotificationProducer =
    util::ringbuffer::Producer<NotificationRingBufferExtraData>;
using TNotificationConsumer =
    util::ringbuffer::Consumer<NotificationRingBufferExtraData>;

} // namespace shm
} // namespace transport
} // namespace tensorpipe
