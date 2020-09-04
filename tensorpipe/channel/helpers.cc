/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/helpers.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/nop.h>

namespace tensorpipe {
namespace channel {

Channel::TDescriptor saveDescriptor(const AbstractNopHolder& object) {
  const size_t len = object.getSize();
  Channel::TDescriptor out(len, '\0');
  nop::BufferWriter writer(reinterpret_cast<uint8_t*>(out.data()), len);

  nop::Status<void> status = object.write(writer);
  TP_THROW_ASSERT_IF(status.has_error())
      << "Error saving descriptor: " << status.GetErrorMessage();

  return out;
}

void loadDescriptor(AbstractNopHolder& object, const Channel::TDescriptor& in) {
  const size_t len = in.size();
  nop::BufferReader reader(reinterpret_cast<const uint8_t*>(in.data()), len);

  nop::Status<void> status = object.read(reader);
  TP_THROW_ASSERT_IF(status.has_error())
      << "Error loading descriptor: " << status.GetErrorMessage();
}

} // namespace channel
} // namespace tensorpipe
