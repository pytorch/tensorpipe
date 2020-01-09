#pragma once

#include <memory>
#include <string>
#include <vector>

#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>

namespace tensorpipe {

class Context final {
 public:
  using address_t = std::string;

  Context(std::vector<std::string>);

  std::shared_ptr<Pipe> connect(address_t);

  std::shared_ptr<Listener> listen(std::vector<address_t>);
};

} // namespace tensorpipe