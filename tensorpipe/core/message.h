/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace tensorpipe {

// Messages consist of a primary buffer and zero or more separate
// buffers. The primary buffer is always a host-side memory region that
// contains a serialized version of the message we're dealing with. This
// serialized message, in turn, may have references to the separate
// buffers that accompany the primary buffer. These separate buffers may
// point to any type of memory, host-side or device-side.
//
class Message final {
 public:
  Message() = default;

  // Messages are movable.
  Message(Message&&) = default;
  Message& operator=(Message&&) = default;

  // But they are not copyable.
  Message(const Message&) = delete;
  Message& operator=(const Message&) = delete;

  std::string metadata;

  struct Payload {
    void* data{nullptr};
    size_t length{0};

    // Users may include arbitrary metadata in the following fields.
    // This may contain allocation hints for the receiver, for example.
    std::string metadata;
  };

  // Holds the payloads that are transferred over the primary connection.
  std::vector<Payload> payloads;

  struct Tensor {
    void* data{nullptr};
    size_t length{0};

    // Users may include arbitrary metadata in the following fields.
    // This may contain allocation hints for the receiver, for example.
    std::string metadata;
  };

  // Holds the tensors that are offered to the side channels.
  std::vector<Tensor> tensors;
};

} // namespace tensorpipe
