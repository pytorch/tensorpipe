#include <tensorpipe/core/listener.h>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/defs.h>

namespace tensorpipe {

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Context> context,
    const std::vector<std::string>& addrs) {
  std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
      transportListeners;
  for (const auto& addr : addrs) {
    std::string scheme;
    std::string host; // FIXME Pick a better name
    std::tie(scheme, host) = splitSchemeOfAddress(addr);
    transportListeners.emplace(
        scheme, context->getContextForScheme_(scheme)->listen(host));
  }
  auto listener = std::make_shared<Listener>(
      ConstructorToken(), std::move(context), std::move(transportListeners));
  listener->start_();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::unordered_map<std::string, std::shared_ptr<transport::Listener>>
        listeners)
    : context_(std::move(context)), listeners_(std::move(listeners)) {}

void Listener::start_() {
  for (const auto& listener : listeners_) {
    armListener_(listener.first);
  }
}

void Listener::accept(accept_callback_fn fn) {
  acceptCallback_.arm(runIfAlive(
      *this,
      std::function<void(Listener&, const Error&, std::shared_ptr<Pipe>)>(
          [fn{std::move(fn)}](
              Listener& listener,
              const Error& error,
              std::shared_ptr<Pipe> pipe) {
            listener.context_->callCallback_(
                [fn{std::move(fn)}, error, pipe{std::move(pipe)}]() {
                  fn(error, std::move(pipe));
                });
          })));
}

void Listener::onAccept_(std::shared_ptr<transport::Connection> connection) {
  auto pipe = std::make_shared<Pipe>(
      Pipe::ConstructorToken(), context_, std::move(connection));
  pipe->start_();
  acceptCallback_.trigger(Error::kSuccess, std::move(pipe));
}

void Listener::armListener_(std::string scheme) {
  auto iter = listeners_.find(scheme);
  if (iter == listeners_.end()) {
    TP_THROW_EINVAL() << "got unsupported scheme: " << scheme;
  }
  auto transportListener = iter->second;
  transportListener->accept(runIfAlive(
      *this,
      std::function<void(Listener&, std::shared_ptr<transport::Connection>)>(
          [scheme](
              Listener& listener,
              std::shared_ptr<transport::Connection> connection) {
            listener.onAccept_(std::move(connection));
            listener.armListener_(scheme);
          })));
}

} // namespace tensorpipe
