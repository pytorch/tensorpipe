#include <signal.h>

// One-time init to use EPIPE errors instead of SIGPIPE
namespace {

struct Initializer {
  explicit Initializer() {
    signal(SIGPIPE, SIG_IGN);
  }
};

Initializer initializer;

} // namespace
