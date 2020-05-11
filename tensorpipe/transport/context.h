/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/defs.h>
#include <tensorpipe/transport/listener.h>

namespace tensorpipe {
namespace transport {

class Context {
 public:
  virtual std::shared_ptr<Connection> connect(address_t addr) = 0;

  virtual std::shared_ptr<Listener> listen(address_t addr) = 0;

  // Return string to describe the domain for this context.
  //
  // Two processes with a context of the same type whose domain
  // descriptors are identical can connect to each other.
  //
  // For example, for a transport that leverages TCP/IP, this may be
  // as simple as the address family (assuming we can route between
  // any two processes). For a transport that leverages shared memory,
  // this descriptor must uniquely identify the machine, such that
  // only co-located processes generate the same domain descriptor.
  //
  virtual const std::string& domainDescriptor() const = 0;

  // Tell the context what its identifier is.
  //
  // This is only supposed to be called from the high-level context or from
  // channel contexts. It will only used for logging and debugging purposes.
  virtual void setId(std::string id) = 0;

  virtual void close() = 0;

  virtual void join() = 0;

  virtual ~Context() = default;
};

} // namespace transport
} // namespace tensorpipe
