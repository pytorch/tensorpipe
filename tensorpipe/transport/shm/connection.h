#pragma once

#include <memory>
#include <mutex>

#include <tensorpipe/transport/connection.h>
#include <tensorpipe/transport/shm/loop.h>
#include <tensorpipe/transport/shm/socket.h>

namespace tensorpipe {
namespace transport {
namespace shm {

class Connection final : public transport::Connection,
                         public std::enable_shared_from_this<Connection>,
                         public EventHandler {
 public:
  Connection(std::shared_ptr<Loop> loop, std::shared_ptr<Socket> socket);

  ~Connection() override;

  // Implementation of transport::Connection.
  void read(read_callback_fn fn) override;

  // Implementation of transport::Connection.
  void read(void* ptr, size_t length, read_callback_fn fn) override;

  // Implementation of transport::Connection
  void write(const void* ptr, size_t length, write_callback_fn fn) override;

  // Implementation of EventHandler.
  void handleEvents(int events) override;

 private:
  std::mutex mutex_;
  std::shared_ptr<Loop> loop_;
  std::shared_ptr<Socket> socket_;

  // Close connection.
  void close();

  // Close connection while holding mutex.
  void closeHoldingMutex();
};

} // namespace shm
} // namespace transport
} // namespace tensorpipe
