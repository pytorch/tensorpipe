/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <tensorpipe/channel/channel_boilerplate.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace channel {

template <typename TCtx, typename TChan>
class ContextImplBoilerplate : public virtual DeferredExecutor,
                               public std::enable_shared_from_this<TCtx> {
 public:
  explicit ContextImplBoilerplate(
      std::unordered_map<Device, std::string> deviceDescriptors);

  ContextImplBoilerplate(const ContextImplBoilerplate&) = delete;
  ContextImplBoilerplate(ContextImplBoilerplate&&) = delete;
  ContextImplBoilerplate& operator=(const ContextImplBoilerplate&) = delete;
  ContextImplBoilerplate& operator=(ContextImplBoilerplate&&) = delete;

  void init();

  virtual size_t numConnectionsNeeded() const;

  const std::unordered_map<Device, std::string>& deviceDescriptors() const;

  virtual bool canCommunicateWithRemote(
      const std::string& localDeviceDescriptor,
      const std::string& remoteDeviceDescriptor) const;

  // Enrolling dependent objects (channels) causes them to be kept alive for as
  // long as the context exists. These objects should enroll themselves as soon
  // as they're created (in their initImplFromLoop method) and unenroll
  // themselves after they've completed handling an error (either right in the
  // handleErrorImpl method or in a subsequent callback). The context, on the
  // other hand, should avoid terminating (i.e., complete joining) until all
  // objects have unenrolled themselves.
  void enroll(TChan& channel);
  void unenroll(TChan& channel);

  // Return whether the context is in a closed state. To avoid race conditions,
  // this must be called from within the loop.
  bool closed();

  void setId(std::string id);

  void close();

  void join();

  virtual ~ContextImplBoilerplate() = default;

 protected:
  virtual void initImplFromLoop() {}
  virtual void handleErrorImpl() = 0;
  virtual void joinImpl() = 0;
  virtual void setIdImpl() {}

  void setError(Error error);

  template <typename... Args>
  std::shared_ptr<Channel> createChannelInternal(Args&&... args);

  Error error_{Error::kSuccess};

  // An identifier for the context, composed of the identifier for the context,
  // combined with the channel's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

  CallbackWrapper<TCtx> callbackWrapper_{*this, *this};

 private:
  void initFromLoop();
  void closeFromLoop();

  void handleError();

  std::atomic<bool> joined_{false};

  const std::unordered_map<Device, std::string> deviceDescriptors_;

  // Sequence numbers for the channels created by this context, used to create
  // their identifiers based off this context's identifier. They will only be
  // used for logging and debugging.
  std::atomic<uint64_t> channelCounter_{0};

  // Store shared_ptrs to dependent objects that have enrolled themselves to
  // keep them alive. We use a map, indexed by raw pointers, rather than a set
  // of shared_ptrs so that we can erase objects without them having to create
  // a fresh shared_ptr just for that.
  std::unordered_map<TChan*, std::shared_ptr<TChan>> channels_;

  // For some odd reason it seems we need to use a qualified name here...
  template <typename T>
  friend class tensorpipe::CallbackWrapper;
};

template <typename TCtx, typename TChan>
ContextImplBoilerplate<TCtx, TChan>::ContextImplBoilerplate(
    std::unordered_map<Device, std::string> deviceDescriptors)
    : deviceDescriptors_(std::move(deviceDescriptors)) {}

template <typename TCtx, typename TChan>
template <typename... Args>
std::shared_ptr<Channel> ContextImplBoilerplate<TCtx, TChan>::
    createChannelInternal(Args&&... args) {
  std::string channelId = id_ + ".c" + std::to_string(channelCounter_++);
  TP_VLOG(4) << "Channel context " << id_ << " is opening channel "
             << channelId;
  return std::make_shared<ChannelBoilerplate<TCtx, TChan>>(
      typename ChannelImplBoilerplate<TCtx, TChan>::ConstructorToken(),
      this->shared_from_this(),
      std::move(channelId),
      std::forward<Args>(args)...);
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::init() {
  deferToLoop([this]() { initFromLoop(); });
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::initFromLoop() {
  TP_DCHECK(inLoop());

  TP_DCHECK(!error_);

  initImplFromLoop();
}

template <typename TCtx, typename TChan>
size_t ContextImplBoilerplate<TCtx, TChan>::numConnectionsNeeded() const {
  return 1;
}

template <typename TCtx, typename TChan>
const std::unordered_map<Device, std::string>& ContextImplBoilerplate<
    TCtx,
    TChan>::deviceDescriptors() const {
  return deviceDescriptors_;
}

template <typename TCtx, typename TChan>
bool ContextImplBoilerplate<TCtx, TChan>::canCommunicateWithRemote(
    const std::string& localDeviceDescriptor,
    const std::string& remoteDeviceDescriptor) const {
  return localDeviceDescriptor == remoteDeviceDescriptor;
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::enroll(TChan& channel) {
  TP_DCHECK(inLoop());
  bool wasInserted;
  std::tie(std::ignore, wasInserted) =
      channels_.emplace(&channel, channel.shared_from_this());
  TP_DCHECK(wasInserted);
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::unenroll(TChan& channel) {
  TP_DCHECK(inLoop());
  auto numRemoved = channels_.erase(&channel);
  TP_DCHECK_EQ(numRemoved, 1);
}

template <typename TCtx, typename TChan>
bool ContextImplBoilerplate<TCtx, TChan>::closed() {
  TP_DCHECK(inLoop());
  return error_;
};

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::setId(std::string id) {
  TP_VLOG(4) << "Channel context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
  setIdImpl();
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::close() {
  deferToLoop([this]() { closeFromLoop(); });
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::closeFromLoop() {
  TP_DCHECK(inLoop());
  TP_VLOG(4) << "Channel context " << id_ << " is closing";
  setError(TP_CREATE_ERROR(ContextClosedError));
  TP_VLOG(4) << "Channel context " << id_ << " done closing";
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::setError(Error error) {
  // Don't overwrite an error that's already set.
  if (error_ || !error) {
    return;
  }

  error_ = std::move(error);

  handleError();
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::handleError() {
  TP_DCHECK(inLoop());
  TP_VLOG(5) << "Channel context " << id_ << " is handling error "
             << error_.what();

  // Make a copy as they could unenroll themselves inline.
  auto channelsCopy = channels_;
  // We call closeFromLoop, rather than just close, because we need these
  // objects to transition _immediately_ to error, "atomically". If we just
  // deferred closing to later, this could come after some already-enqueued
  // operations that could try to access the context, which would be closed,
  // and this could fail.
  for (auto& iter : channelsCopy) {
    iter.second->closeFromLoop();
  }

  handleErrorImpl();
}

template <typename TCtx, typename TChan>
void ContextImplBoilerplate<TCtx, TChan>::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(4) << "Channel context " << id_ << " is joining";

    // As closing is deferred to the loop, we must wait for closeImpl to be
    // actually called before we call joinImpl, to avoid race conditions. For
    // this, we defer another task to the loop, which we know will run after the
    // closing, and then we wait for that task to be run.
    std::promise<void> hasClosed;
    deferToLoop([&]() { hasClosed.set_value(); });
    hasClosed.get_future().wait();

    joinImpl();

    TP_VLOG(4) << "Channel context " << id_ << " done joining";

    // FIXME This may actually not be true, as channels could for example be
    // kept alive by the underlying transport, and thus outlive their context.
    // TP_DCHECK(channels_.empty());
  }
}

} // namespace channel
} // namespace tensorpipe
