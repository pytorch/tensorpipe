/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <string>
#include <utility>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {

template <typename TImpl>
class ContextImplBoilerplate : public virtual DeferredExecutor,
                               public std::enable_shared_from_this<TImpl> {
 public:
  ContextImplBoilerplate(std::string domainDescriptor);

  const std::string& domainDescriptor() const;

  ClosingEmitter& getClosingEmitter();

  void setId(std::string id);

  void close();

  void join();

  virtual ~ContextImplBoilerplate() = default;

 protected:
  virtual void closeImpl() = 0;
  virtual void joinImpl() = 0;

  // An identifier for the context, composed of the identifier for the context,
  // combined with the transport's name. It will only be used for logging and
  // debugging purposes.
  std::string id_{"N/A"};

 private:
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};
  ClosingEmitter closingEmitter_;

  const std::string domainDescriptor_;
};

template <typename TImpl>
ContextImplBoilerplate<TImpl>::ContextImplBoilerplate(
    std::string domainDescriptor)
    : domainDescriptor_(std::move(domainDescriptor)) {}

template <typename TImpl>
const std::string& ContextImplBoilerplate<TImpl>::domainDescriptor() const {
  return domainDescriptor_;
}

template <typename TImpl>
ClosingEmitter& ContextImplBoilerplate<TImpl>::getClosingEmitter() {
  return closingEmitter_;
};

template <typename TImpl>
void ContextImplBoilerplate<TImpl>::setId(std::string id) {
  TP_VLOG(7) << "Transport context " << id_ << " was renamed to " << id;
  id_ = std::move(id);
}

template <typename TImpl>
void ContextImplBoilerplate<TImpl>::close() {
  if (!closed_.exchange(true)) {
    TP_VLOG(7) << "Transport context " << id_ << " is closing";

    closingEmitter_.close();
    closeImpl();

    TP_VLOG(7) << "Transport context " << id_ << " done closing";
  }
}

template <typename TImpl>
void ContextImplBoilerplate<TImpl>::join() {
  close();

  if (!joined_.exchange(true)) {
    TP_VLOG(7) << "Transport context " << id_ << " is joining";

    joinImpl();

    TP_VLOG(7) << "Transport context " << id_ << " done joining";
  }
}

} // namespace transport
} // namespace tensorpipe
