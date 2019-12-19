#pragma once

#include <functional>

namespace tensorpipe {
namespace transport {

class Connection {
 public:
  class Error final {
   public:
    explicit Error() {}

    operator bool() const {
      return false;
    }
  };

  virtual ~Connection() = default;

  using read_callback_fn =
      std::function<void(const Error& error, const void* ptr, size_t len)>;

  virtual void read(read_callback_fn fn) = 0;

  virtual void read(void* ptr, size_t length, read_callback_fn fn) = 0;

  using write_callback_fn = std::function<void(const Error& error)>;

  virtual void write(const void* ptr, size_t length, write_callback_fn fn) = 0;
};

} // namespace transport
} // namespace tensorpipe
