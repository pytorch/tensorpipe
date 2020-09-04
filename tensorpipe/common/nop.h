/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

// The helpers to perform type erasure of the object type: a untemplated base
// class exposing the methods we need for (de)serialization, and then templated
// subclasses allowing to create a holder for each concrete libnop type.

class AbstractNopHolder {
 public:
  virtual size_t getSize() const = 0;
  virtual nop::Status<void> write(nop::BufferWriter& writer) const = 0;
  virtual nop::Status<void> read(nop::BufferReader& reader) = 0;
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

  nop::Status<void> write(nop::BufferWriter& writer) const override {
    return nop::Encoding<T>::Write(object_, &writer);
  }

  nop::Status<void> read(nop::BufferReader& reader) override {
    return nop::Encoding<T>::Read(&object_, &reader);
  }

 private:
  T object_;
};

} // namespace tensorpipe
