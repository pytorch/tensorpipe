#pragma once

#include <functional>
#include <memory>

namespace tensorpipe {
namespace transport {

class Connection;

class Listener {
 public:
  virtual ~Listener() = default;

  using accept_callback_fn = std::function<void(std::shared_ptr<Connection>)>;

  virtual void accept(accept_callback_fn fn) = 0;
};

} // namespace transport
} // namespace tensorpipe
