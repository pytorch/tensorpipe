include(CheckCXXSourceCompiles)
include(CMakePushCheckState)

# We use the [[nodiscard]] attribute, which GCC 5 complains about.
# Silence this warning if GCC 5 is used.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6)
    add_definitions("-Wno-attributes")
  endif()
endif()

# Check libc contains process_vm_readv
CMAKE_PUSH_CHECK_STATE(RESET)
set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS}")
CHECK_CXX_SOURCE_COMPILES("
#include <sys/uio.h>
#include <sys/types.h>
#include <unistd.h>

namespace {
bool isProcessVmReadvSyscallAllowed() {
  long long someSourceValue = 0x0123456789abcdef;
  long long someTargetValue = 0;
  struct iovec source = {
    .iov_base = &someSourceValue, .iov_len = sizeof(someSourceValue)
  };
  struct iovec target = {
    .iov_base = &someTargetValue, .iov_len = sizeof(someTargetValue)
  };
  ssize_t nread = ::process_vm_readv(::getpid(), &target, 1, &source, 1, 0);
  return nread == sizeof(long long) && someTargetValue == someSourceValue;
}
}

int main() {
  isProcessVmReadvSyscallAllowed();
  return 0;
}" SUPPORT_GLIBCXX_USE_PROCESS_VM_READV)
if(NOT SUPPORT_GLIBCXX_USE_PROCESS_VM_READV)
  unset(SUPPORT_GLIBCXX_USE_PROCESS_VM_READV CACHE)
  message(FATAL_ERROR
      "The C++ compiler does not support required functions. "
      "This likely due to an old version of libc.so is used. "
      "Please check your system linker setting.")
endif()
CMAKE_POP_CHECK_STATE()

