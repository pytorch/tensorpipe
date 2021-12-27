/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/channel/helpers.h>

#include <string>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/nop.h>

namespace tensorpipe {
namespace channel {

std::string saveDescriptor(const AbstractNopHolder& object) {
  const size_t len = object.getSize();
  std::string out(len, '\0');
  NopWriter writer(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(out.data())), len);

  nop::Status<void> status = object.write(writer);
  TP_THROW_ASSERT_IF(status.has_error())
      << "Error saving descriptor: " << status.GetErrorMessage();

  return out;
}

void loadDescriptor(AbstractNopHolder& object, const std::string& in) {
  const size_t len = in.size();
  NopReader reader(reinterpret_cast<const uint8_t*>(in.data()), len);

  nop::Status<void> status = object.read(reader);
  TP_THROW_ASSERT_IF(status.has_error())
      << "Error loading descriptor: " << status.GetErrorMessage();
}

} // namespace channel
} // namespace tensorpipe
