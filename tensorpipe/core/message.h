#pragma once

#include <cstddef>
#include <vector>

namespace tensorpipe {

// Messages consist of a primary buffer and zero or more separate
// buffers. The primary buffer is always a host-side memory region that
// contains a serialized version of the message we're dealing with. This
// serialized message, in turn, may have references to the separate
// buffers that accompany the primary buffer. These separate buffers may
// point to any type of memory, host-side or device-side.
//
struct Message final {
  void* ptr{nullptr};
  size_t length{0};

  struct Tensor {
    void* ptr{nullptr};
    size_t length{0};

    // Users may include arbitrary metadata in the following fields.
    // This may contain allocation hints for the receiver, for example.
    const void* user_ptr{nullptr};
    size_t user_length{0};
  };

  // Holds the tensors that are offered to the side channels.
  std::vector<Tensor> tensors;

  // Opaque pointer to be used by downstream callers. May be used to
  // ensure the memory pointed to by this message is kept alive.
  void* privdata;
};

} // namespace tensorpipe
