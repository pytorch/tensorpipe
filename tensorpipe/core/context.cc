#include <iostream>

#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>

namespace tensorpipe {

Context::Context() {
  std::cout << "Initialized a core context" << std::endl;
}

std::shared_ptr<Pipe> Context::connect(std::vector<std::string> addrs) {
  std::cout << "Creating a pipe for addresses:" << std::endl;
  for (auto& addr : addrs) {
    std::cout << "- " << addr << std::endl;
  }
  return std::shared_ptr<Pipe>();
}

std::shared_ptr<Listener> Context::listen(std::vector<std::string> addrs) {
  std::cout << "Creating a listener for addresses:" << std::endl;
  for (auto& addr : addrs) {
    std::cout << "- " << addr << std::endl;
  }
  return std::shared_ptr<Listener>();
}

} // namespace tensorpipe
