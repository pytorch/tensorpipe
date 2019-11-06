#pragma once

#include <memory>

#include <tensorpipe/core/pipe.h>

namespace tensorpipe {

// The listener.
//
// Listeners are used to produce pipes. Depending on the type of the
// context, listeners may use a variety of addresses to listen on. For
// example, for TCP/IP sockets they listen on an IPv4 or IPv6 address,
// for Unix domain sockets they listen on a path, etcetera.
//
// A pipe can only be accepted from this listener after it has been
// fully established. This means that both its connection and all its
// side channels have been established.
//
class Listener {
 public:
  virtual ~Listener();

  virtual std::shared_ptr<Pipe> accept() = 0;
};

} // namespace tensorpipe
