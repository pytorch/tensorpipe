/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <memory>
#include <string>
#include <vector>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <tensorpipe/channel/basic/basic.h>
#ifdef TP_ENABLE_CMA
#include <tensorpipe/channel/cma/cma.h>
#endif // TP_ENABLE_CMA
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/message.h>
#include <tensorpipe/core/pipe.h>
#ifdef TP_ENABLE_SHM
#include <tensorpipe/transport/shm/context.h>
#endif // TP_ENABLE_SHM
#include <tensorpipe/transport/uv/context.h>

namespace py = pybind11;

namespace {

using tensorpipe::nullopt;
using tensorpipe::optional;

// RAII wrapper to reliably release every buffer we get.
class BufferWrapper {
 public:
  BufferWrapper(const py::buffer& buffer, int flags) {
    if (PyObject_GetBuffer(buffer.ptr(), &buffer_, flags) != 0) {
      throw py::error_already_set();
    }
  }

  BufferWrapper(const BufferWrapper& other) = delete;

  BufferWrapper(BufferWrapper&& other) = delete;

  BufferWrapper& operator=(const BufferWrapper& other) = delete;

  BufferWrapper& operator=(BufferWrapper&& other) = delete;

  ~BufferWrapper() {
    PyBuffer_Release(&buffer_);
  }

  void* ptr() const {
    return buffer_.buf;
  }

  size_t length() const {
    return buffer_.len;
  }

  py::buffer_info getBuffer() {
    return py::buffer_info(
        buffer_.buf,
        1,
        py::format_descriptor<unsigned char>::format(),
        1,
        {static_cast<size_t>(buffer_.len)},
        {1});
  }

 private:
  Py_buffer buffer_;
};

class OutgoingTensor {
 public:
  BufferWrapper buffer;
  BufferWrapper metadata;

  OutgoingTensor(const py::buffer& buffer, const py::buffer& metadata)
      : buffer(buffer, PyBUF_SIMPLE), metadata(metadata, PyBUF_SIMPLE) {}
};

class OutgoingMessage {
 public:
  BufferWrapper buffer;
  BufferWrapper metadata;
  std::vector<std::shared_ptr<OutgoingTensor>> tensors;

  OutgoingMessage(
      const py::buffer& buffer,
      const py::buffer& metadata,
      const std::vector<std::shared_ptr<OutgoingTensor>>& tensors)
      : buffer(buffer, PyBUF_SIMPLE),
        metadata(metadata, PyBUF_SIMPLE),
        tensors(tensors) {}
};

tensorpipe::Message prepareToWrite(std::shared_ptr<OutgoingMessage> pyMessage) {
  tensorpipe::Message tpMessage{
      pyMessage->buffer.ptr(),
      pyMessage->buffer.length(),
      {reinterpret_cast<char*>(pyMessage->metadata.ptr()),
       pyMessage->metadata.length()}};
  tpMessage.tensors.reserve(pyMessage->tensors.size());
  for (const auto& pyTensor : pyMessage->tensors) {
    tensorpipe::Message::Tensor tpTensor{
        pyTensor->buffer.ptr(),
        pyTensor->buffer.length(),
        {reinterpret_cast<char*>(pyTensor->metadata.ptr()),
         pyTensor->metadata.length()}};
    tpMessage.tensors.push_back(std::move(tpTensor));
  }
  return tpMessage;
}

class IncomingTensor {
 public:
  size_t length;
  optional<BufferWrapper> buffer;
  py::bytes metadata;

  IncomingTensor(size_t length, py::bytes metadata)
      : length(length), metadata(metadata) {}

  void set_buffer(const py::buffer& pyBuffer) {
    TP_THROW_ASSERT_IF(buffer.has_value()) << "Buffer already set";
    buffer.emplace(pyBuffer, PyBUF_SIMPLE | PyBUF_WRITABLE);
    if (buffer->length() != length) {
      buffer.reset();
      TP_THROW_ASSERT() << "Bad length";
    }
  }
};

class IncomingMessage {
 public:
  size_t length;
  optional<BufferWrapper> buffer;
  py::bytes metadata;
  std::vector<std::shared_ptr<IncomingTensor>> tensors;

  IncomingMessage(
      size_t length,
      py::bytes metadata,
      std::vector<std::shared_ptr<IncomingTensor>> tensors)
      : length(length), metadata(metadata), tensors(tensors) {}

  void set_buffer(const py::buffer& pyBuffer) {
    TP_THROW_ASSERT_IF(buffer.has_value()) << "Buffer already set";
    buffer.emplace(pyBuffer, PyBUF_SIMPLE | PyBUF_WRITABLE);
    if (buffer->length() != length) {
      buffer.reset();
      TP_THROW_ASSERT() << "Bad length";
    }
  }
};

std::shared_ptr<IncomingMessage> prepareToAllocate(
    const tensorpipe::Message& tpMessage) {
  std::vector<std::shared_ptr<IncomingTensor>> pyTensors;
  pyTensors.reserve(tpMessage.tensors.size());
  for (const auto& tpTensor : tpMessage.tensors) {
    TP_DCHECK(tpTensor.data == nullptr);
    pyTensors.push_back(
        std::make_shared<IncomingTensor>(tpTensor.length, tpTensor.metadata));
  }
  TP_DCHECK(tpMessage.data == nullptr);
  auto pyMessage = std::make_shared<IncomingMessage>(
      tpMessage.length, tpMessage.metadata, std::move(pyTensors));
  return pyMessage;
}

tensorpipe::Message prepareToRead(std::shared_ptr<IncomingMessage> pyMessage) {
  TP_THROW_ASSERT_IF(!pyMessage->buffer.has_value()) << "No buffer";
  tensorpipe::Message tpMessage{pyMessage->buffer.value().ptr(),
                                pyMessage->buffer.value().length()};
  tpMessage.tensors.reserve(pyMessage->tensors.size());
  for (const auto& pyTensor : pyMessage->tensors) {
    TP_THROW_ASSERT_IF(!pyTensor->buffer.has_value()) << "No buffer";
    tensorpipe::Message::Tensor tpTensor{pyTensor->buffer.value().ptr(),
                                         pyTensor->buffer.value().length()};
    tpMessage.tensors.push_back(std::move(tpTensor));
  }
  return tpMessage;
}

template <typename T>
using shared_ptr_class_ = py::class_<T, std::shared_ptr<T>>;

template <typename T>
using transport_class_ =
    py::class_<T, tensorpipe::transport::Context, std::shared_ptr<T>>;

template <typename T>
using channel_factory_class_ =
    py::class_<T, tensorpipe::channel::ChannelFactory, std::shared_ptr<T>>;

} // namespace

PYBIND11_MODULE(pytensorpipe, module) {
  py::print(
      "These bindings are EXPERIMENTAL, intended to give a PREVIEW of the API, "
      "and, as such, may CHANGE AT ANY TIME.");

  shared_ptr_class_<tensorpipe::Context> context(module, "Context");
  shared_ptr_class_<tensorpipe::Listener> listener(module, "Listener");
  shared_ptr_class_<tensorpipe::Pipe> pipe(module, "Pipe");

  shared_ptr_class_<OutgoingTensor> outgoingTensor(module, "OutgoingTensor");
  outgoingTensor.def(
      py::init<py::buffer, py::buffer>(),
      py::arg("buffer"),
      py::arg("metadata"));
  shared_ptr_class_<OutgoingMessage> outgoingMessage(module, "OutgoingMessage");
  outgoingMessage.def(
      py::init<
          py::buffer,
          py::buffer,
          const std::vector<std::shared_ptr<OutgoingTensor>>>(),
      py::arg("buffer"),
      py::arg("metadata"),
      py::arg("tensors"));

  shared_ptr_class_<IncomingTensor> incomingTensor(
      module, "IncomingTensor", py::buffer_protocol());
  incomingTensor.def_readonly("length", &IncomingTensor::length);
  incomingTensor.def_readonly("metadata", &IncomingTensor::metadata);
  incomingTensor.def_property(
      "buffer",
      [](IncomingTensor& pyTensor) -> py::buffer_info {
        TP_THROW_ASSERT_IF(!pyTensor.buffer.has_value()) << "No buffer";
        return pyTensor.buffer->getBuffer();
      },
      &IncomingTensor::set_buffer);
  shared_ptr_class_<IncomingMessage> incomingMessage(
      module, "IncomingMessage", py::buffer_protocol());
  incomingMessage.def_readonly("length", &IncomingMessage::length);
  incomingMessage.def_readonly("metadata", &IncomingMessage::metadata);
  incomingMessage.def_readonly("tensors", &IncomingMessage::tensors);
  incomingMessage.def_property(
      "buffer",
      [](IncomingMessage& pyMessage) -> py::buffer_info {
        TP_THROW_ASSERT_IF(!pyMessage.buffer.has_value()) << "No buffer";
        return pyMessage.buffer->getBuffer();
      },
      &IncomingMessage::set_buffer);

  // Creators.

  context.def(py::init(&tensorpipe::Context::create));
  context.def(
      "listen",
      [](std::shared_ptr<tensorpipe::Context> context,
         const std::vector<std::string>& urls) {
        return context->listen(urls);
      },
      py::arg("urls"));
  context.def(
      "connect",
      [](std::shared_ptr<tensorpipe::Context> context, const std::string& url) {
        return context->connect(url);
      },
      py::arg("url"));

  context.def(
      "join",
      &tensorpipe::Context::join,
      py::call_guard<py::gil_scoped_release>());

  // Callback registration.

  listener.def(
      "listen",
      [](std::shared_ptr<tensorpipe::Listener> listener, py::object callback) {
        listener->accept([callback{std::move(callback)}](
                             const tensorpipe::Error& error,
                             std::shared_ptr<tensorpipe::Pipe> pipe) mutable {
          if (error) {
            TP_LOG_ERROR() << error.what();
            return;
          }
          TP_THROW_ASSERT_IF(!pipe) << "No pipe";
          py::gil_scoped_acquire acquire;
          try {
            callback(std::move(pipe));
          } catch (const py::error_already_set& err) {
            TP_LOG_ERROR() << "Callback raised exception: " << err.what();
          }
          // Leaving the scope will decrease the refcount of callback which
          // may cause it to get destructed, which might segfault since we
          // won't be holding the GIL anymore. So we reset callback now,
          // while we're still holding the GIL.
          callback = py::object();
        });
      });

  pipe.def(
      "read_descriptor",
      [](std::shared_ptr<tensorpipe::Pipe> pipe, py::object callback) {
        pipe->readDescriptor([callback{std::move(callback)}](
                                 const tensorpipe::Error& error,
                                 tensorpipe::Message message) mutable {
          if (error) {
            TP_LOG_ERROR() << error.what();
            return;
          }
          py::gil_scoped_acquire acquire;
          try {
            callback(prepareToAllocate(std::move(message)));
          } catch (const py::error_already_set& err) {
            TP_LOG_ERROR() << "Callback raised exception: " << err.what();
          }
          // Leaving the scope will decrease the refcount of callback which
          // may cause it to get destructed, which might segfault since we
          // won't be holding the GIL anymore. So we reset callback now,
          // while we're still holding the GIL.
          callback = py::object();
        });
      });

  pipe.def(
      "read",
      [](std::shared_ptr<tensorpipe::Pipe> pipe,
         std::shared_ptr<IncomingMessage> pyMessage,
         py::object callback) {
        tensorpipe::Message tpMessage = prepareToRead(std::move(pyMessage));
        pipe->read(
            std::move(tpMessage),
            [callback{std::move(callback)}](
                const tensorpipe::Error& error,
                tensorpipe::Message tpMessage) mutable {
              if (error) {
                TP_LOG_ERROR() << error.what();
                return;
              }
              py::gil_scoped_acquire acquire;
              try {
                callback();
              } catch (const py::error_already_set& err) {
                TP_LOG_ERROR() << "Callback raised exception: " << err.what();
              }
              // Leaving the scope will decrease the refcount of callback which
              // may cause it to get destructed, which might segfault since we
              // won't be holding the GIL anymore. So we reset callback now,
              // while we're still holding the GIL.
              callback = py::object();
            });
      });

  pipe.def(
      "write",
      [](std::shared_ptr<tensorpipe::Pipe> pipe,
         std::shared_ptr<OutgoingMessage> pyMessage,
         py::object callback) {
        tensorpipe::Message tpMessage = prepareToWrite(std::move(pyMessage));
        pipe->write(
            std::move(tpMessage),
            [callback{std::move(callback)}](
                const tensorpipe::Error& error,
                tensorpipe::Message tpMessage) mutable {
              if (error) {
                TP_LOG_ERROR() << error.what();
                return;
              }
              py::gil_scoped_acquire acquire;
              try {
                callback();
              } catch (const py::error_already_set& err) {
                TP_LOG_ERROR() << "Callback raised exception: " << err.what();
              }
              // Leaving the scope will decrease the refcount of callback which
              // may cause it to get destructed, which might segfault since we
              // won't be holding the GIL anymore. So we reset callback now,
              // while we're still holding the GIL.
              callback = py::object();
            });
      });

  // Transports and channels

  shared_ptr_class_<tensorpipe::transport::Context> AbstractTransport(
      module, "AbstractTransport");

  transport_class_<tensorpipe::transport::uv::Context> uvTransport(
      module, "UvTransport");
  uvTransport.def(py::init<>());

#ifdef TP_ENABLE_SHM
  transport_class_<tensorpipe::transport::shm::Context> shmTransport(
      module, "ShmTransport");
  shmTransport.def(py::init<>());
#endif // TP_ENABLE_SHM

  context.def(
      "register_transport",
      &tensorpipe::Context::registerTransport,
      py::arg("priority"),
      py::arg("name"),
      py::arg("transport"));

  shared_ptr_class_<tensorpipe::channel::ChannelFactory> AbstractChannel(
      module, "AbstractChannel");

  channel_factory_class_<tensorpipe::channel::basic::BasicChannelFactory>
      basicChannel(module, "BasicChannel");
  basicChannel.def(py::init<>());

#ifdef TP_ENABLE_CMA
  channel_factory_class_<tensorpipe::channel::cma::CmaChannelFactory>
      processVmReadvChannel(module, "CmaChannel");
  processVmReadvChannel.def(
      py::init(&tensorpipe::channel::cma::CmaChannelFactory::create));
#endif // TP_ENABLE_CMA

  context.def(
      "register_channel",
      &tensorpipe::Context::registerChannelFactory,
      py::arg("priority"),
      py::arg("name"),
      py::arg("channel"));

  // Helpers

  listener.def("get_url", &tensorpipe::Listener::url, py::arg("transport"));
}
