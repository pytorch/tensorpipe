#include <tensorpipe/util/shm/segment.h>

#include <fcntl.h>
#include <linux/mman.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>

// XXX: Eventually replace this with std's filesystem.
#include <ghc/filesystem.hpp>

namespace tensorpipe {
namespace util {
namespace shm {

namespace {

namespace fs = ::ghc::filesystem;

fs::path nameToPath(const std::string& name) {
  auto path = fs::path(name);
  if (path.is_absolute()) {
    TP_THROW_EINVAL() << "\"" << path
                      << "\" is absolute path, a relative one was expected.";
  }
  auto new_path = fs::path(Segment::kBasePath) / path;
  TP_ARG_CHECK_EQ(fs::weakly_canonical(new_path), new_path);
  return new_path;
}

void createDirectories(const fs::path& rel_path, mode_t new_dir_mode) {
  // Create all directories in path.
  // Permissions for folders allow anyone to read or write.
  // Individual files inside folders can set their own permissions.

  // Temporarily set umask to zero to set the folder mask to what we need.
  mode_t old_mask = ::umask(0);

  fs::path base_path = Segment::kBasePath;

  auto check_error = [&](int ret) {
    int err = errno;
    if (ret == -1 && err != EEXIST) {
      // Restore permissions mask.
      ::umask(old_mask);
      TP_THROW_SYSTEM(err) << "Unexpected error creating "
                           << "shared memory directory at " << base_path;
    }
  };

  {
    int ret = ::mkdir(base_path.c_str(), new_dir_mode);
    if (ret == -1 && errno != EEXIST) {
      check_error(ret);
    }
  }
  for (const auto& p : rel_path) {
    base_path /= p;
    TP_ARG_CHECK_EQ(fs::weakly_canonical(base_path), base_path);
    int ret = ::mkdir(base_path.c_str(), new_dir_mode);
    check_error(ret);
  }

  // Restore permissions mask.
  ::umask(old_mask);
}

[[nodiscard]] int openOrCreateShmFd(
    const std::string& name,
    bool perm_write,
    const optional<CreationMode>& creation_mode) {
  if (creation_mode.has_value()) {
    createDirectories(fs::path(name).parent_path(), creation_mode->dir);
  }

  // Simplify bookkeeping by not allowing symlinks inside.
  // Note that segments are not closed on exec.
  int fd, flags = O_NOFOLLOW;
  mode_t old_mask;
  // There is always read permission.
  if (perm_write) {
    flags |= O_RDWR;
  } else {
    flags |= O_RDONLY;
  }

  auto full_path = nameToPath(name);
  if (creation_mode.has_value()) {
    // Temporarily set umask to zero to avoid interfering with the
    // mode passed to open.
    old_mask = umask(0);
    if (!(creation_mode->link_flags & CreationMode::LinkOnCreation)) {
      // If link should not happen on creation, create an anonymous
      // temporary file on the directory that eventually will contain
      // the file.
      flags |= O_TMPFILE;
      full_path.remove_filename();
    } else {
      // Set when no using O_TMPFILE to allow file to be linked later.
      flags |= O_CREAT | O_EXCL;
    }
    fd = ::open(full_path.c_str(), flags, creation_mode->shm_file);
    // Restore umask.
    umask(old_mask);
  } else {
    // Load shared memory file.
    fd = ::open(full_path.c_str(), flags);
  }

  if (fd == -1) {
    TP_THROW_SYSTEM(errno) << "Failed to open shared memory file descriptor "
                           << "at " << Segment::kBasePath << "/" << name;
  }
  return fd;
}

// if <byte_size> == 0, map the whole file.
void* mmapShmFd(int fd, size_t byte_size, bool perm_write, PageType page_type) {
#ifdef MAP_SHARED_VALIDATE
  int flags = MAP_SHARED | MAP_SHARED_VALIDATE;
#else

#warning \
    "this version of libc doesn't define MAP_SHARED_VALIDATE, \
update to obtain the latest correctness checks."

  int flags = MAP_SHARED;
#endif
  // Note that in x86 PROT_WRITE implies PROT_READ so there is no
  // point on allowing read protection to be specified.
  // Currently no handling PROT_EXEC because there is no use case for it.
  int prot = PROT_READ;
  if (perm_write) {
    prot |= PROT_WRITE;
  }

  switch (page_type) {
    case PageType::Default:
      break;
    case PageType::HugeTLB_2MB:
      prot |= MAP_HUGETLB | MAP_HUGE_2MB;
      break;
    case PageType::HugeTLB_1GB:
      prot |= MAP_HUGETLB | MAP_HUGE_1GB;
      break;
  }

  void* shm = ::mmap(nullptr, byte_size, prot, flags, fd, 0);
  if (shm == MAP_FAILED) {
    TP_THROW_SYSTEM(errno) << "Failed to mmap memory of size: " << byte_size;
  }

  return shm;
}

} // namespace

// Mention static constexpr char to export the symbol.
constexpr char Segment::kBasePath[];

Segment::Segment(
    const std::string& name,
    bool perm_write,
    optional<PageType> page_type,
    optional<CreationMode> creation_mode)
    : name_{name}, creation_mode_{creation_mode}, base_ptr_{nullptr} {
  bool create = creation_mode_.has_value();

  fd_ = openOrCreateShmFd(name, perm_write, creation_mode);

  // Resize or obtain size.
  if (create) {
    // grow size to contain byte_size bytes.
    off_t len = static_cast<off_t>(creation_mode_->byte_size);
    int ret = ::fallocate(fd_, 0, 0, len);
    if (ret == -1) {
      TP_THROW_SYSTEM(errno)
          << "Error while allocating " << creation_mode_->byte_size
          << " bytes in shared memory"
          << " file " << name;
    }
    this->byte_size_ = creation_mode_->byte_size;
  } else {
    // Load whole file. Use fstat to obtain size.
    struct stat sb;
    int ret = ::fstat(fd_, &sb);
    if (ret == -1) {
      TP_THROW_SYSTEM(errno) << "Error while fstat shared memory file.";
    }
    this->byte_size_ = static_cast<size_t>(sb.st_size);
  }

  // If page_type is not set, use the default.
  page_type_ = page_type.value_or(getDefaultPageType(this->byte_size_));
  this->base_ptr_ = mmapShmFd(fd_, byte_size_, perm_write, page_type_);
}

void Segment::link() {
  auto path = nameToPath(name_);
  if (linked_) {
    TP_THROW_SYSTEM(EEXIST) << "Segment already linked at: \"" << path << "\"";
  }
  auto sym_path = "/proc/self/fd/" + std::to_string(fd_);
  // This links the unnamed temporary file.
  // For details, see man 2 open 's  O_TMPFILE section.
  int ret = ::linkat(
      AT_FDCWD, sym_path.c_str(), AT_FDCWD, path.c_str(), AT_SYMLINK_FOLLOW);
  if (unlikely(ret != 0)) {
    TP_THROW_SYSTEM(-ret) << "Failed to link file descriptor " << fd_ << " ("
                          << sym_path << ") to path: " << path
                          << ". Is path already in use?";
  }
  linked_ = true;
}

// Remove all empty directories from path until kBasePath.
int removeEmptyDirectories(fs::path path) noexcept {
  auto stop_path = fs::path(Segment::kBasePath);

  while (path != stop_path) {
    int ret = ::rmdir(path.c_str());
    if (ret == -1) {
      if (errno == ENOTEMPTY || errno == EEXIST) {
        return 0;
      } else {
        return -errno;
      }
    }
    path = path.parent_path();
  }
  return 0;
}

int unlinkNoExcept(const std::string& name) noexcept {
  auto path = nameToPath(name);
  if (fs::is_directory(path)) {
    return -EISDIR;
  }
  int ret = ::unlink(path.c_str());
  if (ret == -1) {
    return -errno;
  }
  return removeEmptyDirectories(path.parent_path());
}

void Segment::unlink(const std::string& name) {
  int ret = unlinkNoExcept(name);
  if (ret < 0) {
    TP_THROW_SYSTEM_CODE(toErrorCode(-ret))
        << "Error unliking shared memory segment";
  }
}

Segment::~Segment() {
  int ret = munmap(base_ptr_, byte_size_);
  if (ret == -1) {
    TP_LOG_ERROR() << "Error while munmapping shared memory segment. Error: "
                   << toErrorCode(errno).message();
  }
  if (creation_mode_.has_value() &&
      (creation_mode_->link_flags & CreationMode::UnlinkOnDestruction) &&
      linked_) {
    ret = unlinkNoExcept(name_);
    if (ret < 0) {
      TP_LOG_ERROR() << "Error unlinking shared memory segment. Error: "
                     << toErrorCode(-ret).message();
    }
  }
  if (0 > fd_) {
    TP_LOG_ERROR() << "Attempt to destroy segment with negative file "
                   << "descriptor";
    return;
  }
  ::close(fd_);
}

std::string Segment::createTempDir(const std::string& prefix) {
  createDirectories(prefix, CreationMode::kDefaultDirMode);

  auto p = nameToPath(prefix);
  // Add characters to be replaced by mkdtemp.
  p /= "XXXXXX";
  // Create temporal storage for new path.
  auto temp = std::vector<char>(p.string().length() + 1, '\0');
  // Copy template.
  ::strncpy(temp.data(), p.string().c_str(), temp.size());
  // Create new folder.
  char* new_path = ::mkdtemp(temp.data());
  TP_THROW_SYSTEM_IF(new_path == nullptr, errno)
      << "Error creating dirs: " << p;

  std::string name{new_path};
  // Change directory mode
  int ret = ::chmod(name.c_str(), CreationMode::kDefaultDirMode);
  TP_THROW_SYSTEM_IF(ret != 0, -ret) << "Error changing mode of temp dir";

  // Remove kBasePath and slash.
  // strlen is safe because this are string literals.
  return name.substr(sizeof(kBasePath)) + "/";
}

} // namespace shm
} // namespace util
} // namespace tensorpipe
