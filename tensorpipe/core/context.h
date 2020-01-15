#pragma once

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/queue.h>
#include <tensorpipe/transport/context.h>

namespace tensorpipe {

class Context final {
  // The constructor needs to be public (so that make_shared can invoke it) but
  // in order to prevent external users from calling it (to force them to use
  // the `create` static member function) we make it accept an instance of this
  // private class.
  struct ConstructorToken {};

 public:
  static std::shared_ptr<Context> create(const std::vector<std::string>&);

  Context(ConstructorToken, const std::vector<std::string>&);

  void join();

 private:
  std::unordered_map<std::string, std::shared_ptr<transport::Context>>
      contexts_;

  std::shared_ptr<transport::Context> getContextForScheme_(std::string);

  std::thread callbackCaller_;
  Queue<optional<std::function<void()>>> callbackQueue_;

  void start_();
  void runCallbackCaller_();
  void callCallback_(std::function<void()>);

  friend class Listener;
  friend class Pipe;
};

} // namespace tensorpipe