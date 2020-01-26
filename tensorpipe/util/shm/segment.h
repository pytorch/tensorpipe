#pragma once

#include <fcntl.h>
#include <cstring>
#include <memory>
#include <sstream>

#include <tensorpipe/common/defs.h>
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

/// Choose a reasonable page size for a given size.
/// This very opinionated choice of "reasonable" aims to
/// keep wasted memory low.
// XXX: Lots of memory could be wasted if size is slightly larger than
// page size, handle that case.
constexpr PageType getDefaultPageType(uint64_t size) {
  constexpr uint64_t MB = 1024ull * 1024ull;
  constexpr uint64_t GB = 1024ull * MB;

  if (size >= (15ul * GB) / 16ul) {
    // At least 15/16 of a 1GB page (at most 64 MB wasted).
    return PageType::HugeTLB_1GB;
  } else if (size >= ((2ul * MB) * 3ul) / 4ul) {
    // At least 3/4 of a 2MB page (at most 512 KB wasteds)
    return PageType::HugeTLB_2MB;
  } else {
    // No Huge TLB page, use Default (4KB for x86).
    return PageType::Default;
  }
}

/// Parameters to create a new Segment.
struct CreationMode {
  // Default mode for directories created in shared memory file system.
  static constexpr mode_t kDefaultDirMode = S_IRWXU | S_IRWXG | S_IRWXO;

  enum LinkFlags {
    None = 0x0,
    // Link to file path right after creation
    // (do not require an explicit link call).
    LinkOnCreation = 0x1,
    // Unlink when Segment object is destroyed.
    UnlinkOnDestruction = 0x2
  };

  size_t byte_size;
  LinkFlags link_flags;
  mode_t dir; // Creation mode for directories that contain shared memory file.
  mode_t shm_file; // Creation mode for shared memory files.

  // Factory function to make CreatioMode with permissive modes.
  static auto allPerms(size_t byte_size, LinkFlags link_flags) {
    return CreationMode{
        .byte_size = byte_size,
        .link_flags = link_flags,
        .dir = kDefaultDirMode,
        .shm_file = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH};
  }
};

class Segment {
 public:
  // Default base path for all segments created.
  static constexpr char kBasePath[] = "/dev/shm";

  Segment(const Segment&) = delete;
  Segment(Segment&&) = delete;

  /// If creation_mode == nullopt, then shm file must
  /// already exists, otherwise, create directory path
  /// and new shm file using modes in <creation_mode>.
  Segment(
      const std::string& name,
      bool perm_write,
      optional<PageType> page_type,
      optional<CreationMode> creation_mode);

  /// Create read and size shared memory to contain an object of class T.
  ///
  /// The created object's shared_ptr will own the lifetime of the
  /// Segment and will call Segment destructor.
  /// Caller can use the shared_ptr to the underlying Segment.
  template <class T, class... Args>
  static std::pair<std::shared_ptr<T>, std::shared_ptr<Segment>> create(
      const std::string& name,
      bool perm_write,
      optional<PageType> page_type,
      CreationMode::LinkFlags link_flags,
      Args&&... args) {
    static_assert(
        !std::is_array<T>::value,
        "Did you mean to use the array version of Segment::create");
    static_assert(std::is_trivially_copyable<T>::value, "!");

    const auto byte_size = sizeof(T);
    auto creation_mode = CreationMode::allPerms(byte_size, link_flags);
    auto segment =
        std::make_shared<Segment>(name, perm_write, page_type, creation_mode);
    TP_DCHECK_EQ(segment->getSize(), byte_size);

    // Initialize in place. Forward T's constructor arguments.
    T* ptr = new (segment->getPtr()) T(std::forward<Args>(args)...);
    if (ptr != segment->getPtr()) {
      TP_THROW_SYSTEM(EPERM)
          << "new's address cannot be different from segment->getPtr() "
          << " address. Some aligment assumption was incorrect";
    }

    return {std::shared_ptr<T>(segment, ptr), segment};
  }

  /// One-dimensional array version of create<T, ...Args>.
  /// Caller can use the shared_ptr to the underlying Segment.
  // XXX: Fuse all versions of create.
  template <class T>
  static std::pair<std::shared_ptr<T>, std::shared_ptr<Segment>> create(
      size_t num_elements,
      const std::string& name,
      bool perm_write,
      optional<PageType> page_type,
      CreationMode::LinkFlags link_flags) {
    static_assert(
        std::is_trivially_copyable<T>::value,
        "Shared memory segments are restricted to only store objects that "
        "are trivially copyable (i.e. no pointers and no heap allocation");

    static_assert(
        std::is_array<T>::value,
        "Did you mean to use the non-array version, Segment::create<T, ...>");

    static_assert(
        std::rank<T>::value == 1,
        "Currently, only one-dimensional arrays are supported. "
        "You can use the non-template version of Segment::create");

    using TScalar = typename std::remove_extent<T>::type;
    static_assert(std::is_trivially_copyable<TScalar>::value, "!");
    static_assert(!std::is_array<TScalar>::value, "!");
    static_assert(std::is_same<TScalar[], T>::value, "Type mismatch");

    size_t byte_size = sizeof(TScalar) * num_elements;
    auto creation_mode = CreationMode::allPerms(byte_size, link_flags);
    auto segment =
        std::make_shared<Segment>(name, perm_write, page_type, creation_mode);
    TP_DCHECK_EQ(segment->getSize(), byte_size);

    // Initialize in place.
    TScalar* ptr = new (segment->getPtr()) TScalar[num_elements]();
    if (ptr != segment->getPtr()) {
      TP_THROW_SYSTEM(EPERM)
          << "new's address cannot be different from segment->getPtr() "
          << " address. Some aligment assumption was incorrect";
    }

    return {std::shared_ptr<TScalar[]>(segment, ptr), segment};
  }

  /// Load an already created shared memory Segment that holds an
  /// object of type T, where T is an array type.
  ///
  /// Lifecycle of shared_ptr and Segment's reference_wrapper is
  /// identical to create<>().
  template <class T, std::enable_if_t<std::is_array<T>::value, int> = 0>
  static std::pair<std::shared_ptr<T>, std::shared_ptr<Segment>> load(
      const std::string& name,
      bool perm_write,
      optional<PageType> page_type) {
    auto segment =
        std::make_shared<Segment>(name, perm_write, page_type, nullopt);
    const size_t size = segment->getSize();
    static_assert(
        std::rank<T>::value == 1,
        "Currently only rank one arrays are supported");
    using TScalar = typename std::remove_extent<T>::type;
    static_assert(std::is_trivially_copyable<TScalar>::value, "!");
    auto ptr = static_cast<TScalar*>(segment->getPtr());
    return {std::shared_ptr<T>(segment, ptr), segment};
  }

  /// Load an already created shared memory Segment that holds an
  /// object of type T, where T is NOT an array type.
  ///
  /// Lifecycle of shared_ptr and Segment's reference_wrapper is
  /// identical to create<>().
  template <class T, std::enable_if_t<!std::is_array<T>::value, int> = 0>
  static std::pair<std::shared_ptr<T>, std::shared_ptr<Segment>> load(
      const std::string& name,
      bool perm_write,
      optional<PageType> page_type) {
    auto segment =
        std::make_shared<Segment>(name, perm_write, page_type, nullopt);
    const size_t size = segment->getSize();
    // XXX: Do some checking other than the size that we are loading
    // the right type.
    if (size != sizeof(T)) {
      TP_THROW_SYSTEM(EPERM)
          << "Shared memory file has unexpected size. "
          << "Got: " << size << " bytes, expected: " << sizeof(T) << ". "
          << "If there is a race between creation and loading of segments, "
          << "consider linking segment after it has been fully initialized.";
    }
    static_assert(std::is_trivially_copyable<T>::value, "!");
    auto ptr = static_cast<T*>(segment->getPtr());
    return {std::shared_ptr<T>(segment, ptr), segment};
  }

  /// Make Segment's underlying shm file visible to others in file path
  /// provided at construction.
  ///
  /// This is implemented following man 2 open in O_TMPFILE,
  /// Uses O_TMPFILE and delayed linking to avoid race conditions between
  /// file initialization and opening by other processes.
  void link();

  /// Auxiliar function to unlink a file segment. Useful when creator
  /// has set <unlink_on_destruction> to false.
  static void unlink(const std::string& name);

  /// Create a temporal directory whose path starts with prefix.
  static std::string createTempDir(const std::string& prefix);

  void* getPtr() {
    return base_ptr_;
  }

  const void* getPtr() const {
    return base_ptr_;
  }

  size_t getSize() const {
    return byte_size_;
  }

  PageType getPageType() const {
    return page_type_;
  }

  ~Segment();

 protected:
  // Unique identifier for memory segment. It must be a canonical path.
  const std::string name_;

  // If true, unlink the file when Segment is destroyed.
  const optional<CreationMode> creation_mode_;

  // The page used to mmap the segment.
  PageType page_type_;

  // Base pointer of mmmap'ed shared memory segment.
  void* base_ptr_;

  // Byte size of shared memory segment.
  size_t byte_size_;

  // The file descriptor of the shared memory file.
  int fd_ = -1;

  bool linked_ = false;
};

} // namespace shm
} // namespace util
} // namespace tensorpipe
