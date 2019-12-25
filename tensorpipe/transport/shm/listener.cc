#include <tensorpipe/transport/shm/listener.h>

#include <tensorpipe/common/defs.h>

namespace tensorpipe {
namespace transport {
namespace shm {

Listener::Listener(
    std::shared_ptr<Loop> loop,
    const Sockaddr& addr,
    connection_callback_fn fn)
    : loop_(std::move(loop)),
      listener_(Socket::createForFamily(AF_UNIX)),
      fn_(std::move(fn)) {
  // Bind socket to abstract socket address.
  listener_->bind(addr);
  listener_->block(false);
  listener_->listen(128);

  // Register with loop for readability events.
  loop_->registerDescriptor(listener_->fd(), EPOLLIN, this);
}

Listener::~Listener() {
  if (listener_) {
    loop_->unregisterDescriptor(listener_->fd());
  }
}

void Listener::handleEvents(int events) {
  TP_ARG_CHECK_EQ(events, EPOLLIN);

  for (;;) {
    auto socket = listener_->accept();
    if (!socket) {
      break;
    }

    fn_(std::make_shared<Connection>(loop_, socket));
  }
}

} // namespace shm
} // namespace transport
} // namespace tensorpipe
