#include <tensorpipe/core/pipe.h>

#include <cstring>

#include <tensorpipe/common/address.h>
#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/error_macros.h>
#include <tensorpipe/proto/message_descriptor.pb.h>

namespace tensorpipe {

namespace {

void writeProtobufToConnection(
    std::shared_ptr<transport::Connection> connection,
    google::protobuf::Message* pb,
    std::function<void(const transport::Error&)> fn) {
  const auto len = pb->ByteSize(); // FIXME use ByteSizeLong
  // Using a unique_ptr instead of this shared_ptr because if the lambda
  // captures a unique_ptr then it becomes non-copyable, which prevents it from
  // being converted to a function.
  // In C++20 use std::make_shared<uint8_t[]>(len).
  // Note: this is a std::shared_ptr<uint8_t[]> semantically.
  // A shared_ptr with array type is supported in C++17 and higher.
  auto buf = std::shared_ptr<uint8_t>(
      new uint8_t[len], std::default_delete<uint8_t[]>());
  auto ptr = buf.get();
  TP_DCHECK_EQ(pb->SerializeWithCachedSizesToArray(ptr), ptr + len)
      << "couldn't serialize Protobuf message";
  connection->write(
      ptr,
      len,
      [buf{std::move(buf)}, fn{std::move(fn)}](const transport::Error& error) {
        fn(error);
        // The buffer will be destructed when this function returns.
      });
}

} // namespace

//
// Initialization
//

std::shared_ptr<Pipe> Pipe::create(
    std::shared_ptr<Context> context,
    const std::string& addr) {
  std::string scheme;
  std::string host; // FIXME Pick a better name
  std::tie(scheme, host) = splitSchemeOfAddress(addr);
  auto pipe = std::make_shared<Pipe>(
      ConstructorToken(),
      std::move(context),
      context->getContextForScheme_(scheme)->connect(host));
  pipe->start_();
  return pipe;
}

Pipe::Pipe(
    ConstructorToken /* unused */,
    std::shared_ptr<Context> context,
    std::shared_ptr<transport::Connection> connection)
    : context_(std::move(context)), connection_(std::move(connection)) {
  isRunOfScheduledCallbacksTriggered_.clear();
}

void Pipe::start_() {
  armRead_();
}

//
// Entry points for user code
//

void Pipe::readDescriptor(read_descriptor_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    triggerReadDescriptorCallback_(std::move(fn), error_, Message());
    return;
  }
  readDescriptorCallback_.arm(runIfAlive(
      *this,
      std::function<void(Pipe&, const Error&, Message&&)>(
          [fn{std::move(fn)}](
              Pipe& pipe, const Error& error, Message&& message) mutable {
            pipe.waitingDescriptors_.push_back(message.copyWithoutData());
            pipe.triggerReadDescriptorCallback_(
                std::move(fn), error, std::move(message));
          })));
}

void Pipe::read(Message&& message, read_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    triggerReadCallback_(std::move(fn), error_, std::move(message));
    return;
  }
  // FIXME This should throw an exception or pass an error to the callback
  TP_DCHECK(!waitingDescriptors_.empty())
      << "called read with no waiting descriptor";
  Message expectedMessage{std::move(waitingDescriptors_.front())};
  waitingDescriptors_.pop_front();
  // TODO Compare message with expectedMessage
  proto::Packet pbPacket;
  proto::Request* pbRequest = pbPacket.mutable_request();
  writeProtobufToConnection(
      connection_,
      &pbPacket,
      runIfAlive(
          *this,
          std::function<void(Pipe&, const transport::Error&)>(
              [](Pipe& pipe, const transport::Error& error) {
                if (pipe.error_) {
                  return;
                }
                if (error) {
                  pipe.error_ = TP_CREATE_ERROR(TransportError, error);
                  pipe.flushEverythingOnError_();
                  return;
                }
              })));
  pendingReads_.emplace_back(std::move(message), std::move(fn));
}

void Pipe::write(Message&& message, write_callback_fn fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    triggerWriteCallback_(std::move(fn), error_, std::move(message));
    return;
  }
  proto::Packet pbPacket;
  proto::MessageDescriptor* pbMessageDesc =
      pbPacket.mutable_message_descriptor();
  pbMessageDesc->set_size_in_bytes(message.length);
  for (const auto& tensor : message.tensors) {
    proto::MessageDescriptor::TensorDescriptor* pbTensorDesc =
        pbMessageDesc->add_tensor_descriptors();
    pbTensorDesc->set_device_type(proto::DeviceType::DEVICE_TYPE_CPU);
    pbTensorDesc->set_size_in_bytes(tensor.length);
    pbTensorDesc->set_user_data(tensor.metadata);
  }
  writeProtobufToConnection(
      connection_,
      &pbPacket,
      runIfAlive(
          *this,
          std::function<void(Pipe&, const transport::Error&)>(
              [](Pipe& pipe, const transport::Error& error) {
                if (pipe.error_) {
                  return;
                }
                if (error) {
                  pipe.error_ = TP_CREATE_ERROR(TransportError, error);
                  pipe.flushEverythingOnError_();
                  return;
                }
              })));
  pendingWrites_.emplace_back(std::move(message), std::move(fn));
}

//
// Entry points for callbacks from transports
//

void Pipe::onRead_(const transport::Error& error, const void* ptr, size_t len) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (error_) {
    return;
  }
  if (error) {
    error_ = TP_CREATE_ERROR(TransportError, error);
    flushEverythingOnError_();
    return;
  }
  proto::Packet pbPacket;
  TP_DCHECK(pbPacket.ParseFromArray(ptr, len)) << "couldn't parse packet";
  if (pbPacket.has_message_descriptor()) {
    const proto::MessageDescriptor& pbMessageDesc =
        pbPacket.message_descriptor();
    Message message;
    message.length = pbMessageDesc.size_in_bytes();
    for (const auto& pbTensorDesc : pbMessageDesc.tensor_descriptors()) {
      Message::Tensor tensor;
      tensor.length = pbTensorDesc.size_in_bytes();
      tensor.metadata = std::move(pbTensorDesc.user_data());
      message.tensors.push_back(std::move(tensor));
    }
    readDescriptorCallback_.trigger(Error::kSuccess, std::move(message));
  } else if (pbPacket.has_message()) {
    const proto::Message& pbMessage = pbPacket.message();
    TP_DCHECK(!pendingReads_.empty()) << "got message when no pending reads";
    Message message{std::move(std::get<0>(pendingReads_.front()))};
    read_callback_fn fn{std::move(std::get<1>(pendingReads_.front()))};
    pendingReads_.pop_front();
    std::memcpy(message.data.get(), pbMessage.data().data(), message.length);
    TP_DCHECK_EQ(pbMessage.tensors_size(), message.tensors.size())
        << "mismatch in number of tensors";
    for (int i = 0; i < message.tensors.size(); i += 1) {
      const proto::Message::Tensor& pbTensor = pbMessage.tensors(i);
      Message::Tensor& tensor = message.tensors[i];
      std::memcpy(tensor.data.get(), pbTensor.data().data(), tensor.length);
    }
    triggerReadCallback_(std::move(fn), Error::kSuccess, std::move(message));
  } else if (pbPacket.has_request()) {
    TP_DCHECK(!pendingWrites_.empty()) << "got request when no pending writes";
    Message message{std::move(std::get<0>(pendingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(pendingWrites_.front()))};
    pendingWrites_.pop_front();
    proto::Packet pbPacket;
    proto::Message* pbMessage = pbPacket.mutable_message();
    pbMessage->set_data(message.data.get(), message.length);
    for (const auto& tensor : message.tensors) {
      proto::Message::Tensor* pbTensor = pbMessage->add_tensors();
      pbTensor->set_data(tensor.data.get(), tensor.length);
    }
    writeProtobufToConnection(
        connection_,
        &pbPacket,
        runIfAlive(
            *this,
            std::function<void(Pipe&, const transport::Error&)>(
                [](Pipe& pipe, const transport::Error& error) {
                  if (pipe.error_) {
                    return;
                  }
                  if (error) {
                    pipe.error_ = TP_CREATE_ERROR(TransportError, error);
                    pipe.flushEverythingOnError_();
                    return;
                  }
                  TP_DCHECK(!pipe.completingWrites_.empty())
                      << "got message when no completing writes";
                  Message message =
                      std::move(std::get<0>(pipe.completingWrites_.front()));
                  write_callback_fn fn =
                      std::move(std::get<1>(pipe.completingWrites_.front()));
                  pipe.completingWrites_.pop_front();
                  pipe.triggerWriteCallback_(
                      std::move(fn), Error::kSuccess, std::move(message));
                })));
    completingWrites_.emplace_back(std::move(message), std::move(fn));
  } else {
    TP_LOG_ERROR() << "packet has no payload";
  }
  armRead_();
}

//
// Helpers to prepare callbacks from transports
//

void Pipe::armRead_() {
  connection_->read(runIfAlive(
      *this,
      std::function<void(Pipe&, const transport::Error&, const void*, size_t)>(
          [](Pipe& pipe,
             const transport::Error& error,
             const void* ptr,
             size_t len) { pipe.onRead_(error, ptr, len); })));
}

//
// Helpers to schedule our callbacks into user code
//

void Pipe::triggerReadDescriptorCallback_(
    read_descriptor_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledReadDescriptorCallbacks_.schedule(
      std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerReadCallback_(
    read_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledReadCallbacks_.schedule(std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerWriteCallback_(
    write_callback_fn&& fn,
    const Error& error,
    Message&& message) {
  scheduledWriteCallbacks_.schedule(std::move(fn), error, std::move(message));
  triggerRunOfScheduledCallbacks_();
}

void Pipe::triggerRunOfScheduledCallbacks_() {
  if (!isRunOfScheduledCallbacksTriggered_.test_and_set()) {
    context_->callCallback_(
        runIfAlive(*this, std::function<void(Pipe&)>([](Pipe& pipe) {
          pipe.isRunOfScheduledCallbacksTriggered_.clear();
          pipe.runScheduledCallbacks_();
        })));
  }
}

void Pipe::runScheduledCallbacks_() {
  scheduledReadDescriptorCallbacks_.run();
  scheduledReadCallbacks_.run();
  scheduledWriteCallbacks_.run();
}

//
// Error handling
//

void Pipe::flushEverythingOnError_() {
  readDescriptorCallback_.triggerIfArmed(error_, Message());
  waitingDescriptors_.clear();
  while (!pendingReads_.empty()) {
    Message message{std::move(std::get<0>(pendingReads_.front()))};
    read_callback_fn fn{std::move(std::get<1>(pendingReads_.front()))};
    pendingReads_.pop_front();
    scheduledReadCallbacks_.schedule(std::move(fn), error_, std::move(message));
  }
  while (!pendingWrites_.empty()) {
    Message message{std::move(std::get<0>(pendingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(pendingWrites_.front()))};
    pendingWrites_.pop_front();
    scheduledWriteCallbacks_.schedule(
        std::move(fn), error_, std::move(message));
  }
  while (!completingWrites_.empty()) {
    Message message{std::move(std::get<0>(completingWrites_.front()))};
    write_callback_fn fn{std::move(std::get<1>(completingWrites_.front()))};
    completingWrites_.pop_front();
    scheduledWriteCallbacks_.schedule(
        std::move(fn), error_, std::move(message));
  }
  triggerRunOfScheduledCallbacks_();
}

} // namespace tensorpipe
