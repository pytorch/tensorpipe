/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/core/pipe.h>

#include <memory>
#include <string>
#include <utility>

#include <tensorpipe/core/pipe_impl.h>

namespace tensorpipe {

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string remoteName,
    const std::string& url)
    : impl_(std::make_shared<PipeImpl>(
          std::move(context),
          std::move(id),
          std::move(remoteName),
          url)) {
  impl_->init();
}

Pipe::Pipe(ConstructorToken /* unused */, std::shared_ptr<PipeImpl> impl)
    : impl_(std::move(impl)) {}

const std::string& Pipe::getRemoteName() {
  return impl_->getRemoteName();
}

Pipe::~Pipe() {
  close();
}

void Pipe::close() {
  impl_->close();
}

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  impl_->readDescriptor(std::move(fn));
}

void Pipe::read(Allocation allocation, read_callback_fn fn) {
  impl_->read(std::move(allocation), std::move(fn));
}

void Pipe::write(Message message, write_callback_fn fn) {
  impl_->write(std::move(message), std::move(fn));
}

} // namespace tensorpipe
