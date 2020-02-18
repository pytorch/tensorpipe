/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <future>
#include <memory>
#include <vector>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/transport/connection.h>

// Channels are an out of band mechanism to transfer data between
// processes. Examples include a direct address space to address space
// memory copy on the same machine, or a GPU-to-GPU memory copy.
//
// Construction of a channel happens as follows.
//
//   1) During initialization of a pipe, the connecting peer sends its
//      list of channel factories and their domain descriptors. The
//      domain descriptor is used to determine whether or not a
//      channel can be used by a pair of peers.
//   2) The listening side of the pipe compares the list it received
//      its own list to determine the list of channels should be used
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

// Abstract base class for channel classes.
class Channel {
 public:
  using TDescriptor = std::vector<uint8_t>;
  using TSendCallback = std::function<void(const Error&)>;
  using TRecvCallback = std::function<void(const Error&)>;

  enum class Endpoint : bool { kConnect, kListen };

  virtual ~Channel();

  // Send memory region to peer.
  virtual TDescriptor send(
      const void* ptr,
      size_t length,
      TSendCallback callback) = 0;

  // Receive memory region from peer.
  virtual void recv(
      TDescriptor descriptor,
      void* ptr,
      size_t length,
      TRecvCallback callback) = 0;
};

// Abstract base class for channel factory classes.
//
// Instances of these classes are expected to be registered with a
// context. All registered instances are assumed to be eligible
// channels for all pairs.
//
class ChannelFactory {
 public:
  explicit ChannelFactory(std::string name);

  virtual ~ChannelFactory();

  // Return the factory's name.
  const std::string& name() const;

  // Return string to describe the domain for this channel.
  //
  // Two processes with a channel factory of the same type whose
  // domain descriptors are identical can connect to each other.
  //
  virtual const std::string& domainDescriptor() const = 0;

  // Return newly created channel using the specified connection.
  //
  // It is up to the channel to either use this connection for further
  // initialization, or use it directly. Either way, the returned
  // channel should be immediately usable. If the channel isn't fully
  // initialized yet, take care to queue these operations to execute
  // as soon as initialization has completed.
  //
  virtual std::shared_ptr<Channel> createChannel(
      std::shared_ptr<transport::Connection>,
      Channel::Endpoint) = 0;

  virtual void join();

 private:
  std::string name_;
};

} // namespace channel
} // namespace tensorpipe
