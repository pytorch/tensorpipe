/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/util/ringbuffer/ringbuffer.h>
#include <tensorpipe/util/shm/segment.h>

namespace tensorpipe {
namespace util {
namespace ringbuffer {
namespace shm {

/// Creates ringbuffer on shared memory.
///
/// RingBuffer's data can have any <tensorpipe::util::shm::PageType>
/// (e.g. 4KB or a HugeTLB Page of 2MB or 1GB). If  <data_page_type> is not
/// provided, then choose the largest page that would result in
/// close to full occupancy.
///
/// If <persistent>, the shared memory will not be unlinked
/// when RingBuffer is destroyed.
///
/// <min_rb_byte_size> is the minimum size of the data section
/// of a RingBuffer (or each CPU's RingBuffer).
///
std::tuple<int, int, std::shared_ptr<RingBuffer>> create(
    size_t min_rb_byte_size,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true);

std::shared_ptr<RingBuffer> load(
    int header_fd,
    int data_fd,
    optional<tensorpipe::util::shm::PageType> data_page_type = nullopt,
    bool perm_write = true);

} // namespace shm
} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
