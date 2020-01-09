#pragma once

#include <functional>
#include <memory>

namespace tensorpipe {
namespace transport {

class Connection;

class Listener {
 public:
  using connection_callback_fn =
      std::function<void(std::shared_ptr<Connection>)>;

  virtual ~Listener() = default;
};

} // namespace transport
} // namespace tensorpipe
