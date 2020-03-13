/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <tensorpipe/util/ringbuffer/consumer.h>
#include <tensorpipe/util/ringbuffer/producer.h>

#include <google/protobuf/io/zero_copy_stream.h>

namespace tensorpipe {
namespace util {
namespace ringbuffer {

class ZeroCopyInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  ZeroCopyInputStream(Consumer* buffer, size_t payloadSize)
      : buffer_(buffer), payloadSize_(payloadSize) {}

  bool Next(const void** data, int* size) override {
    if (bytesCount_ == payloadSize_) {
      return false;
    }

    std::tie(*size, *data) =
        buffer_->readContiguousAtMostInTx(payloadSize_ - bytesCount_);
    TP_THROW_SYSTEM_IF(*size < 0, -*size);

    bytesCount_ += *size;

    return true;
  }

  void BackUp(int /* unused */) override {
    // This should never be called as we know the size upfront.
    TP_THROW_ASSERT() << "BackUp() called from ZeroCopyInputStream";
  }

  bool Skip(int /* unused */) override {
    // This should never be called as we know the size upfront.
    TP_THROW_ASSERT() << "Skip() called from ZeroCopyInputStream";
    return false;
  }

  int64_t ByteCount() const override {
    return bytesCount_;
  }

 private:
  Consumer* buffer_{nullptr};
  const size_t payloadSize_{0};
  int64_t bytesCount_{0};
};

class ZeroCopyOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  ZeroCopyOutputStream(Producer* buffer, size_t payloadSize)
      : buffer_(buffer), payloadSize_(payloadSize) {}

  bool Next(void** data, int* size) override {
    std::tie(*size, *data) =
        buffer_->reserveContiguousInTx(payloadSize_ - bytesCount_);
    if (*size == -ENOSPC) {
      return false;
    }
    TP_THROW_SYSTEM_IF(*size < 0, -*size);

    bytesCount_ += *size;

    return true;
  }

  void BackUp(int /* unused */) override {
    // This should never be called as we know the size upfront.
    TP_THROW_ASSERT() << "BackUp() called from ZeroCopyOutputStream";
  }

  int64_t ByteCount() const override {
    return bytesCount_;
  }

 private:
  Producer* buffer_{nullptr};
  const size_t payloadSize_{0};
  int64_t bytesCount_{0};
};

} // namespace ringbuffer
} // namespace util
} // namespace tensorpipe
