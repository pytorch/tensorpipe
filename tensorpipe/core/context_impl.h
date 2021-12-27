/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <tensorpipe/channel/context.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class ListenerImpl;
class PipeImpl;

class ContextImpl final : public virtual DeferredExecutor,
                          public std::enable_shared_from_this<ContextImpl> {
 public:
  explicit ContextImpl(ContextOptions opts);

  void init();

  void registerTransport(
      int64_t priority,
      std::string transport,
      std::shared_ptr<transport::Context> context);

  void registerChannel(
      int64_t priority,
      std::string channel,
      std::shared_ptr<channel::Context> context);

  std::shared_ptr<Listener> listen(const std::vector<std::string>& urls);

  std::shared_ptr<Pipe> connect(const std::string& url, PipeOptions opts);

  std::shared_ptr<transport::Context> getTransport(
      const std::string& transport);
  std::shared_ptr<channel::Context> getChannel(const std::string& channel);

  using TOrderedTransports = std::map<
      int64_t,
      std::tuple<std::string, std::shared_ptr<transport::Context>>>;

  const TOrderedTransports& getOrderedTransports();

  using TOrderedChannels = std::
      map<int64_t, std::tuple<std::string, std::shared_ptr<channel::Context>>>;

  const TOrderedChannels& getOrderedChannels();

  // Return the name given to the context's constructor. It will be retrieved
  // by the pipes and listener in order to attach it to logged messages.
  const std::string& getName();

  // Enrolling dependent objects (listeners and pipes) causes them to be kept
  // alive for as long as the context exists. These objects should enroll
  // themselves as soon as they're created (in their initFromLoop method) and
  // unenroll themselves after they've completed handling an error (either right
  // in the handleError method or in a subsequent callback). The context, on the
  // other hand, should avoid terminating (i.e., complete joining) until all
  // objects have unenrolled themselves.
  void enroll(ListenerImpl& listener);
  void enroll(PipeImpl& pipe);
  void unenroll(ListenerImpl& listener);
  void unenroll(PipeImpl& pipe);

  // Return whether the context is in a closed state. To avoid race conditions,
  // this must be called from within the loop.
  bool closed();

  // Implement DeferredExecutor interface.
  void deferToLoop(TTask fn) override;
  bool inLoop() const override;

  void close();

  void join();

 private:
  OnDemandDeferredExecutor loop_;

  Error error_{Error::kSuccess};

  std::atomic<bool> joined_{false};

  // An identifier for the context, either consisting of the user-provided name
  // for this context (see below) or, by default, composed of unique information
  // about the host and process, combined with an increasing sequence number. It
  // will be used as a prefix for the identifiers of listeners and pipes. All of
  // them will only be used for logging and debugging purposes.
  std::string id_;

  // Sequence numbers for the listeners and pipes created by this context, used
  // to create their identifiers based off this context's identifier. They will
  // only be used for logging and debugging.
  std::atomic<uint64_t> listenerCounter_{0};
  std::atomic<uint64_t> pipeCounter_{0};

  // Store shared_ptrs to dependent objects that have enrolled themselves to
  // keep them alive. We use a map, indexed by raw pointers, rather than a set
  // of shared_ptrs so that we can erase objects without them having to create
  // a fresh shared_ptr just for that.
  std::unordered_map<ListenerImpl*, std::shared_ptr<ListenerImpl>> listeners_;
  std::unordered_map<PipeImpl*, std::shared_ptr<PipeImpl>> pipes_;

  // A user-provided name for this context which should be semantically
  // meaningful. It will only be used for logging and debugging purposes, to
  // identify the endpoints of a pipe.
  std::string name_;

  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      transports_;

  using TContextMap =
      std::unordered_map<std::string, std::shared_ptr<channel::Context>>;
  TContextMap channels_;

  TOrderedTransports transportsByPriority_;

  TOrderedChannels channelsByPriority_;

  CallbackWrapper<ContextImpl> callbackWrapper_{*this, *this};

  void initFromLoop();
  void closeFromLoop();
  void setError(Error error);
  void handleError();

  template <typename T>
  friend class CallbackWrapper;
};

} // namespace tensorpipe
