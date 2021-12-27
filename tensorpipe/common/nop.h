/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <nop/serializer.h>
#include <nop/status.h>
#include <nop/utility/buffer_reader.h>
#include <nop/utility/buffer_writer.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>

namespace tensorpipe {

// Libnop makes heavy use of templates, whereas TensorPipe is designed around
// polymorphism (abstract interfaces and concrete derived classes). The two
// don't mix well: for example, one can't have virtual method templates. One
// technique to get around this is type erasure, which is however tricky to get
// right because the "fundamental" operation(s) of libnop, (de)serialization,
// are simultaneously templated on two types: the reader/writer and the object.
// Ideally we'd like for both these sets of types to be dynamically extensible,
// as we want to allow transpors to provide their own specialized readers and
// writers, and channels could have their own custom objects that they want to
// (de)serialize. New transports and channel could be implemented by third
// parties and plugged in at runtime, so the sets of reader/writers and of
// objects that we must support can't be known in advance.

// We had originally found a solution to this pickle by doing two type erasures
// one after the other, first on the reader/writer, which deals with bytes and
// not objects and is thus not templated, and then on objects, leveraging the
// fact that there is one libnop (de)serializer that takes a *pointer* to a
// reader/writer giving us a "hook" on which to do polymorphism, by hardcoding a
// pointer to the base reader/writer class as template parameter, but then
// passing in an instance of a concrete subclass at runtime.

// However it turned out that this performed poorly, apparently due to the
// (de)serialization process consisting in many small calls to the reader/writer
// which each had to perform a vtable lookup. So, instead, we decided to not
// allow transports to utilize custom specialized readers/writers and to provide
// a single global reader/writer class that is able to cover the two main usage
// patterns we think are most likely to come up: reading/writing to a temporary
// contiguous buffer, and reading/writing to a ringbuffer.

// This reader and writer can operate either on one single buffer (ptr + len) or
// on two buffers: in the latter case, they first consume the first one and,
// when that fills up, they "spill over" into the second one. This is needed in
// order to support the "wrap around" point in ringbuffers.

class NopReader final {
 public:
  NopReader(const uint8_t* ptr, size_t len) : ptr1_(ptr), len1_(len) {}

  NopReader(const uint8_t* ptr1, size_t len1, const uint8_t* ptr2, size_t len2)
      : ptr1_(ptr1), len1_(len1), ptr2_(ptr2), len2_(len2) {}

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Ensure(size_t size) {
    if (likely(size <= len1_ + len2_)) {
      return nop::ErrorStatus::None;
    } else {
      return nop::ErrorStatus::ReadLimitReached;
    }
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Read(uint8_t* byte) {
    if (unlikely(len1_ == 0)) {
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    *byte = *ptr1_;
    ptr1_++;
    len1_--;
    return nop::ErrorStatus::None;
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Read(void* begin, void* end) {
    size_t size =
        reinterpret_cast<uint8_t*>(end) - reinterpret_cast<uint8_t*>(begin);

    if (unlikely(len1_ < size)) {
      std::memcpy(begin, ptr1_, len1_);
      begin = reinterpret_cast<uint8_t*>(begin) + len1_;
      size -= len1_;
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    std::memcpy(begin, ptr1_, size);
    ptr1_ += size;
    len1_ -= size;
    return nop::ErrorStatus::None;
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Skip(size_t paddingBytes) {
    if (unlikely(len1_ < paddingBytes)) {
      paddingBytes -= len1_;
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    ptr1_ += paddingBytes;
    len1_ -= paddingBytes;
    return nop::ErrorStatus::None;
  }

 private:
  const uint8_t* ptr1_ = nullptr;
  size_t len1_ = 0;
  const uint8_t* ptr2_ = nullptr;
  size_t len2_ = 0;
};

class NopWriter final {
 public:
  NopWriter(uint8_t* ptr, size_t len) : ptr1_(ptr), len1_(len) {}
  NopWriter(uint8_t* ptr1, size_t len1, uint8_t* ptr2, size_t len2)
      : ptr1_(ptr1), len1_(len1), ptr2_(ptr2), len2_(len2) {}

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Prepare(size_t size) {
    if (likely(size <= len1_ + len2_)) {
      return nop::ErrorStatus::None;
    } else {
      return nop::ErrorStatus::WriteLimitReached;
    }
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Write(uint8_t byte) {
    if (unlikely(len1_ == 0)) {
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    *ptr1_ = byte;
    ptr1_++;
    len1_--;
    return nop::ErrorStatus::None;
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Write(const void* begin, const void* end) {
    size_t size = reinterpret_cast<const uint8_t*>(end) -
        reinterpret_cast<const uint8_t*>(begin);

    if (unlikely(len1_ < size)) {
      std::memcpy(ptr1_, begin, len1_);
      begin = reinterpret_cast<const uint8_t*>(begin) + len1_;
      size -= len1_;
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    std::memcpy(ptr1_, begin, size);
    ptr1_ += size;
    len1_ -= size;
    return nop::ErrorStatus::None;
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  nop::Status<void> Skip(size_t paddingBytes, uint8_t paddingValue) {
    if (unlikely(len1_ < paddingBytes)) {
      std::memset(ptr1_, paddingValue, paddingBytes);
      paddingBytes -= len1_;
      ptr1_ = ptr2_;
      len1_ = len2_;
      ptr2_ = nullptr;
      len2_ = 0;
    }

    std::memset(ptr1_, paddingValue, paddingBytes);
    ptr1_ += paddingBytes;
    len1_ -= paddingBytes;
    return nop::ErrorStatus::None;
  }

 private:
  uint8_t* ptr1_ = nullptr;
  size_t len1_ = 0;
  uint8_t* ptr2_ = nullptr;
  size_t len2_ = 0;
};

// The helpers to perform type erasure of the object type: a untemplated base
// class exposing the methods we need for (de)serialization, and then templated
// subclasses allowing to create a holder for each concrete libnop type.

class AbstractNopHolder {
 public:
  virtual size_t getSize() const = 0;
  virtual nop::Status<void> write(NopWriter& writer) const = 0;
  virtual nop::Status<void> read(NopReader& reader) = 0;
  virtual ~AbstractNopHolder() = default;
};

template <typename T>
class NopHolder : public AbstractNopHolder {
 public:
  T& getObject() {
    return object_;
  }

  const T& getObject() const {
    return object_;
  }

  size_t getSize() const override {
    return nop::Encoding<T>::Size(object_);
  }

  nop::Status<void> write(NopWriter& writer) const override {
    return nop::Encoding<T>::Write(object_, &writer);
  }

  nop::Status<void> read(NopReader& reader) override {
    return nop::Encoding<T>::Read(&object_, &reader);
  }

 private:
  T object_;
};

} // namespace tensorpipe

namespace nop {

// The `nop::Encoding` specialization for `tensorpipe::optional` was inspired
// by that of `nop::Optional`, available here:
// https://github.com/google/libnop/blob/master/include/nop/base/optional.h
template <typename T>
struct Encoding<tensorpipe::optional<T>> : EncodingIO<tensorpipe::optional<T>> {
  using Type = tensorpipe::optional<T>;

  // NOLINTNEXTLINE(readability-identifier-naming)
  static constexpr EncodingByte Prefix(const Type& value) {
    return value ? Encoding<T>::Prefix(value.value()) : EncodingByte::Nil;
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  static constexpr std::size_t Size(const Type& value) {
    return value ? Encoding<T>::Size(value.value())
                 : BaseEncodingSize(EncodingByte::Nil);
  }

  // NOLINTNEXTLINE(readability-identifier-naming)
  static constexpr bool Match(EncodingByte prefix) {
    return prefix == EncodingByte::Nil || Encoding<T>::Match(prefix);
  }

  template <typename Writer>
  // NOLINTNEXTLINE(readability-identifier-naming)
  static constexpr Status<void> WritePayload(
      EncodingByte prefix,
      const Type& value,
      Writer* writer) {
    if (value) {
      return Encoding<T>::WritePayload(prefix, value.value(), writer);
    } else {
      return {};
    }
  }

  template <typename Reader>
  // NOLINTNEXTLINE(readability-identifier-naming)
  static constexpr Status<void> ReadPayload(
      EncodingByte prefix,
      Type* value,
      Reader* reader) {
    if (prefix == EncodingByte::Nil) {
      value->reset();
    } else {
      T temp;
      auto status = Encoding<T>::ReadPayload(prefix, &temp, reader);
      if (!status) {
        return status;
      }

      *value = std::move(temp);
    }

    return {};
  }
};

} // namespace nop
