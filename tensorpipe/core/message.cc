#include <tensorpipe/core/message.h>

namespace tensorpipe {

Message Message::copyWithoutData() {
  Message copy;
  copy.length = length;
  copy.tensors.reserve(tensors.size());
  for (auto& tensor : tensors) {
    Tensor tensorCopy;
    tensorCopy.length = tensor.length;
    // FIXME Should we copy metadata?
    copy.tensors.push_back(std::move(tensorCopy));
  }
  // FIXME Should we copy privateData?
  return copy;
}

} // namespace tensorpipe
