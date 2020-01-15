#include <tensorpipe/core/context.h>

#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/shm/context.h>

namespace tensorpipe {

std::shared_ptr<Context> Context::create(
    const std::vector<std::string>& transports) {
  auto context =
      std::make_shared<Context>(ConstructorToken(), std::move(transports));
  context->start_();
  return context;
}

Context::Context(
    ConstructorToken /* unused */,
    const std::vector<std::string>& transports)
    : callbackQueue_(1000) {
  for (const auto& transport : transports) {
    if (transport == "shm") {
      contexts_.emplace(transport, std::make_shared<transport::shm::Context>());
    } else {
      TP_THROW_EINVAL() << "unsupported transport";
    }
  }
}

void Context::start_() {
  callbackCaller_ = std::thread([this]() { runCallbackCaller_(); });
}

std::shared_ptr<transport::Context> Context::getContextForScheme_(
    std::string scheme) {
  auto iter = contexts_.find(scheme);
  if (iter == contexts_.end()) {
    TP_THROW_EINVAL() << "addr has unsupported scheme: " << scheme;
  }
  return iter->second;
}

void Context::join() {
  for (auto& context : contexts_) {
    context.second->join();
  }
  callbackQueue_.push(nullopt);
  callbackCaller_.join();
}

void Context::runCallbackCaller_() {
  while (true) {
    auto fn = callbackQueue_.pop();
    if (!fn.has_value()) {
      break;
    }
    fn.value()();
  }
}

void Context::callCallback_(std::function<void()> fn) {
  callbackQueue_.push(std::move(fn));
}

} // namespace tensorpipe
