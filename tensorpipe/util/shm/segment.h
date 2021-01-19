/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fcntl.h>
#include <cstring>
#include <memory>
#include <sstream>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/fd.h>
#include <tensorpipe/common/memory.h>
#include <tensorpipe/common/optional.h>

//
// A C++17 version of shared memory segments handler inspired on boost
// interprocess.
//
// Handles lifetime through shared_ptr custom deleters and allows folders inside
// /dev/shm (Linux only).
//

namespace tensorpipe {
namespace util {
namespace shm {

/// PageType to suggest to Operative System.
/// The final page type depends on system configuration
/// and availability of pages of requested size.
/// HugeTLB pages often need to be reserved at boot time and
/// may none left by the time Segment that request one is cerated.
enum class PageType { Default, HugeTLB_2MB, HugeTLB_1GB };

class Segment {
  Segment(Fd fd, MmappedPtr ptr);

 public:
  Segment() = default;

  static std::tuple<Error, Segment> alloc(
      size_t byteSize,
      bool permWrite,
      optional<PageType> pageType);

  static std::tuple<Error, Segment> access(
      Fd fd,
      bool permWrite,
      optional<PageType> pageType);

  /// Allocate shared memory to contain an object of type T and construct it.
  ///
  /// The Segment object owns the memory and frees it when destructed.
  /// The raw pointer to the object provides a view into the Segment but doesn't
  /// own it and may thus become invalid if the Segment isn't kept alive.
  template <
      typename T,
      typename... Args,
      std::enable_if_t<!std::is_array<T>::value, int> = 0>
  static std::tuple<Error, Segment, T*> create(
      bool permWrite,
      optional<PageType> pageType,
      Args&&... args) {
    static_assert(
        std::is_trivially_copyable<T>::value,
        "Shared memory segments are restricted to only store objects that "
        "are trivially copyable (i.e. no pointers and no heap allocation");

    const auto byteSize = sizeof(T);
    Error error;
    Segment segment;
    std::tie(error, segment) = Segment::alloc(byteSize, permWrite, pageType);
    if (error) {
      return std::make_tuple(std::move(error), Segment(), nullptr);
    }
    TP_DCHECK_EQ(segment.getSize(), byteSize);

    // Initialize in place. Forward T's constructor arguments.
    T* ptr = new (segment.getPtr()) T(std::forward<Args>(args)...);
    TP_THROW_SYSTEM_IF(ptr != segment.getPtr(), EPERM)
        << "new's address cannot be different from segment.getPtr() "
        << "address. Some aligment assumption was incorrect";

    return std::make_tuple(Error::kSuccess, std::move(segment), ptr);
  }

  /// One-dimensional array version of create<T, ...Args>.
  // XXX: Fuse all versions of create.
  template <
      typename T,
      std::enable_if_t<std::is_array<T>::value, int> = 0,
      typename TScalar = typename std::remove_all_extents<T>::type>
  static std::tuple<Error, Segment, TScalar*> create(
      size_t numElements,
      bool permWrite,
      optional<PageType> pageType) {
    static_assert(
        std::is_same<TScalar[], T>::value,
        "Only one-dimensional unbounded arrays are supported");
    static_assert(
        std::is_trivially_copyable<TScalar>::value,
        "Shared memory segments are restricted to only store objects that "
        "are trivially copyable (i.e. no pointers and no heap allocation");

    size_t byteSize = sizeof(TScalar) * numElements;
    Error error;
    Segment segment;
    std::tie(error, segment) = Segment::alloc(byteSize, permWrite, pageType);
    if (error) {
      return std::make_tuple(std::move(error), Segment(), nullptr);
    }
    TP_DCHECK_EQ(segment.getSize(), byteSize);

    // Initialize in place.
    TScalar* ptr = new (segment.getPtr()) TScalar[numElements]();
    TP_THROW_SYSTEM_IF(ptr != segment.getPtr(), EPERM)
        << "new's address cannot be different from segment.getPtr() "
        << "address. Some aligment assumption was incorrect";

    return std::make_tuple(Error::kSuccess, std::move(segment), ptr);
  }

  /// Load an existing shared memory region that already holds an object of type
  /// T, where T is NOT an array type.
  template <typename T, std::enable_if_t<!std::is_array<T>::value, int> = 0>
  static std::tuple<Error, Segment, T*> load(
      Fd fd,
      bool permWrite,
      optional<PageType> pageType) {
    static_assert(
        std::is_trivially_copyable<T>::value,
        "Shared memory segments are restricted to only store objects that "
        "are trivially copyable (i.e. no pointers and no heap allocation");

    Error error;
    Segment segment;
    std::tie(error, segment) =
        Segment::access(std::move(fd), permWrite, pageType);
    if (error) {
      return std::make_tuple(std::move(error), Segment(), nullptr);
    }
    const size_t size = segment.getSize();
    // XXX: Do some checking other than the size that we are loading
    // the right type.
    TP_THROW_SYSTEM_IF(size != sizeof(T), EPERM)
        << "Shared memory file has unexpected size. "
        << "Got: " << size << " bytes, expected: " << sizeof(T) << ". "
        << "If there is a race between creation and loading of segments, "
        << "consider linking segment after it has been fully initialized.";
    auto ptr = static_cast<T*>(segment.getPtr());

    return std::make_tuple(Error::kSuccess, std::move(segment), ptr);
  }

  /// Load an existing shared memory region that already holds an object of type
  /// T, where T is an array type.
  template <
      typename T,
      std::enable_if_t<std::is_array<T>::value, int> = 0,
      typename TScalar = typename std::remove_all_extents<T>::type>
  static std::tuple<Error, Segment, TScalar*> load(
      Fd fd,
      bool permWrite,
      optional<PageType> pageType) {
    static_assert(
        std::is_same<TScalar[], T>::value,
        "Only one-dimensional unbounded arrays are supported");
    static_assert(
        std::is_trivially_copyable<TScalar>::value,
        "Shared memory segments are restricted to only store objects that "
        "are trivially copyable (i.e. no pointers and no heap allocation");

    Error error;
    Segment segment;
    std::tie(error, segment) =
        Segment::access(std::move(fd), permWrite, pageType);
    if (error) {
      return std::make_tuple(std::move(error), Segment(), nullptr);
    }
    auto ptr = static_cast<TScalar*>(segment.getPtr());

    return std::make_tuple(Error::kSuccess, std::move(segment), ptr);
  }

  int getFd() const {
    return fd_.fd();
  }

  void* getPtr() {
    return ptr_.ptr();
  }

  const void* getPtr() const {
    return ptr_.ptr();
  }

  size_t getSize() const {
    return ptr_.getLength();
  }

 private:
  // The file descriptor of the shared memory file.
  Fd fd_;

  // Base pointer of mmmap'ed shared memory segment.
  MmappedPtr ptr_;
};

} // namespace shm
} // namespace util
} // namespace tensorpipe
