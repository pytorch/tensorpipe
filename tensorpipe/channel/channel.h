/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <string>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/buffer.h>
#include <tensorpipe/common/error.h>

// Channels are an out of band mechanism to transfer data between
// processes. Examples include a direct address space to address space
// memory copy on the same machine, or a GPU-to-GPU memory copy.
//
// Construction of a channel happens as follows.
//
//   1) During initialization of a pipe, the connecting peer sends its
//      list of channel contexts and their device descriptors. The
//      device descriptor is used to determine whether or not a
//      channel can be used by a pair of peers.
//   2) The listening side of the pipe compares the list it received
//      its own list to determine the list of channels that should be used
//      for the peers.
//   3) For every channel that should be constructed, the listening
//      side registers a slot with its low level listener. These slots
//      uniquely identify inbound connections on this listener (by
//      sending a word-sized indentifier immediately after connecting)
//      and can be used to construct new connections. These slots are
//      sent to the connecting side of the pipe, which then attempts
//      to establish a new connection for every token.
//   4) At this time, we have a new control connection for every
//      channel that is about to be constructed. Both sides of the
//      pipe can now create the channel instance using the newly
//      created connection. Further initialization that needs to
//      happen is defered to the channel implementation. We assume the
//      channel is usable from the moment it is constructed.
//
namespace tensorpipe {
namespace channel {

using TSendCallback = std::function<void(const Error&)>;
using TRecvCallback = std::function<void(const Error&)>;

// Abstract base class for channel classes.
class Channel {
 public:
  // Send memory region to peer.
  virtual void send(Buffer buffer, size_t length, TSendCallback callback) = 0;

  // Receive memory region from peer.
  virtual void recv(Buffer buffer, size_t length, TRecvCallback callback) = 0;

  // Tell the channel what its identifier is.
  //
  // This is only supposed to be called from the high-level pipe. It will only
  // used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  // Put the channel in a terminal state, aborting pending operations and
  // rejecting future ones, and release its resources. This may be carried out
  // asynchronously, in background.
  virtual void close() = 0;

  virtual ~Channel() = default;
};

} // namespace channel
} // namespace tensorpipe
