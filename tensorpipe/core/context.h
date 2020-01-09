#pragma once

#include <memory>
#include <string>
#include <vector>

namespace tensorpipe {

class Pipe;
class Listener;

class Context {
 public:
  using address_t = std::string;

  Context();

  std::shared_ptr<Pipe> connect(std::vector<address_t> addrs);

  std::shared_ptr<Listener> listen(std::vector<address_t> addrs);
};

} // namespace tensorpipe