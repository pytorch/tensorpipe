#include <tensorpipe/transport/uv/listener.h>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/transport/uv/connection.h>
#include <tensorpipe/transport/uv/uv.h>

namespace tensorpipe {
namespace transport {
namespace uv {

std::shared_ptr<Listener> Listener::create(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr) {
  auto listener =
      std::make_shared<Listener>(ConstructorToken(), std::move(loop), addr);
  listener->start();
  return listener;
}

Listener::Listener(
    ConstructorToken /* unused */,
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr)
    : loop_(std::move(loop)) {
  listener_ = loop_->createHandle<TCPHandle>();
  listener_->bind(addr);
}

Listener::~Listener() {
  if (listener_) {
    listener_->close();
  }
}

void Listener::start() {
  listener_->listen(runIfAlive(
      *this,
      std::function<void(Listener&, int)>([](Listener& self, int status) {
        self.connectionCallback(status);
      })));
}

Sockaddr Listener::addr() {
  return listener_->sockName();
}

void Listener::accept(accept_callback_fn fn) {
  fn_.emplace(std::move(fn));
}

void Listener::connectionCallback(int status) {
  if (status != 0) {
    TP_LOG_WARNING() << "connection callback called with status " << status
                     << ": " << uv_strerror(status);
    return;
  }

  // TODO: accept connection from loop.
  if (fn_) {
    fn_.value()(Connection::create(loop_));
  }
}

} // namespace uv
} // namespace transport
} // namespace tensorpipe
