/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/tensorpipe.h>

namespace py = pybind11;

namespace {

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

class OutgoingPayload {
 public:
  BufferWrapper buffer;
  BufferWrapper metadata;

  OutgoingPayload(const py::buffer& buffer, const py::buffer& metadata)
      : buffer(buffer, PyBUF_SIMPLE), metadata(metadata, PyBUF_SIMPLE) {}
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
  BufferWrapper metadata;
  std::vector<std::shared_ptr<OutgoingPayload>> payloads;
  std::vector<std::shared_ptr<OutgoingTensor>> tensors;

  OutgoingMessage(
      const py::buffer& metadata,
      const std::vector<std::shared_ptr<OutgoingPayload>>& payloads,
      const std::vector<std::shared_ptr<OutgoingTensor>>& tensors)
      : metadata(metadata, PyBUF_SIMPLE),
        payloads(payloads),
        tensors(tensors) {}
};

tensorpipe::Message prepareToWrite(std::shared_ptr<OutgoingMessage> pyMessage) {
  tensorpipe::Message tpMessage{
      {reinterpret_cast<char*>(pyMessage->metadata.ptr()),
       pyMessage->metadata.length()}};
  tpMessage.payloads.reserve(pyMessage->payloads.size());
  for (const auto& pyPayload : pyMessage->payloads) {
    tensorpipe::Message::Payload tpPayload{
        .data = pyPayload->buffer.ptr(),
        .length = pyPayload->buffer.length(),
        .metadata =
            {reinterpret_cast<char*>(pyPayload->metadata.ptr()),
             pyPayload->metadata.length()},
    };
    tpMessage.payloads.push_back(std::move(tpPayload));
  }
  tpMessage.tensors.reserve(pyMessage->tensors.size());
  for (const auto& pyTensor : pyMessage->tensors) {
    tensorpipe::Message::Tensor tpTensor{
        .buffer = tensorpipe::CpuBuffer{.ptr = pyTensor->buffer.ptr()},
        .length = pyTensor->buffer.length(),
        .metadata =
            {reinterpret_cast<char*>(pyTensor->metadata.ptr()),
             pyTensor->metadata.length()},
    };
    tpMessage.tensors.push_back(std::move(tpTensor));
  }
  return tpMessage;
}

class IncomingPayload {
 public:
  size_t length;
  optional<BufferWrapper> buffer;
  py::bytes metadata;

  IncomingPayload(size_t length, py::bytes metadata)
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
  py::bytes metadata;
  std::vector<std::shared_ptr<IncomingPayload>> payloads;
  std::vector<std::shared_ptr<IncomingTensor>> tensors;

  IncomingMessage(
      py::bytes metadata,
      std::vector<std::shared_ptr<IncomingPayload>> payloads,
      std::vector<std::shared_ptr<IncomingTensor>> tensors)
      : metadata(metadata), payloads(payloads), tensors(tensors) {}
};

std::shared_ptr<IncomingMessage> prepareToAllocate(
    const tensorpipe::Descriptor& tpDescriptor) {
  std::vector<std::shared_ptr<IncomingPayload>> pyPayloads;
  pyPayloads.reserve(tpDescriptor.payloads.size());
  for (const auto& tpPayload : tpDescriptor.payloads) {
    pyPayloads.push_back(std::make_shared<IncomingPayload>(
        tpPayload.length, tpPayload.metadata));
  }
  std::vector<std::shared_ptr<IncomingTensor>> pyTensors;
  pyTensors.reserve(tpDescriptor.tensors.size());
  for (const auto& tpTensor : tpDescriptor.tensors) {
    pyTensors.push_back(
        std::make_shared<IncomingTensor>(tpTensor.length, tpTensor.metadata));
  }
  auto pyMessage = std::make_shared<IncomingMessage>(
      tpDescriptor.metadata, std::move(pyPayloads), std::move(pyTensors));
  return pyMessage;
}

tensorpipe::Allocation prepareToRead(
    std::shared_ptr<IncomingMessage> pyMessage) {
  tensorpipe::Allocation tpAllocation;
  tpAllocation.payloads.reserve(pyMessage->payloads.size());
  for (const auto& pyPayload : pyMessage->payloads) {
    TP_THROW_ASSERT_IF(!pyPayload->buffer.has_value()) << "No buffer";
    tensorpipe::Allocation::Payload tpPayload{
        .data = pyPayload->buffer.value().ptr(),
    };
    tpAllocation.payloads.push_back(std::move(tpPayload));
  }
  tpAllocation.tensors.reserve(pyMessage->tensors.size());
  for (const auto& pyTensor : pyMessage->tensors) {
    TP_THROW_ASSERT_IF(!pyTensor->buffer.has_value()) << "No buffer";
    tensorpipe::Allocation::Tensor tpTensor{
        .buffer = tensorpipe::CpuBuffer{.ptr = pyTensor->buffer.value().ptr()},
    };
    tpAllocation.tensors.push_back(std::move(tpTensor));
  }
  return tpAllocation;
}

template <typename T>
using shared_ptr_class_ = py::class_<T, std::shared_ptr<T>>;

} // namespace

PYBIND11_MODULE(pytensorpipe, module) {
  py::print(
      "These bindings are EXPERIMENTAL, intended to give a PREVIEW of the API, "
      "and, as such, may CHANGE AT ANY TIME.");

  shared_ptr_class_<tensorpipe::Context> context(module, "Context");
  shared_ptr_class_<tensorpipe::Listener> listener(module, "Listener");
  shared_ptr_class_<tensorpipe::Pipe> pipe(module, "Pipe");

  shared_ptr_class_<OutgoingPayload> outgoingPayload(module, "OutgoingPayload");
  outgoingPayload.def(
      py::init<py::buffer, py::buffer>(),
      py::arg("buffer"),
      py::arg("metadata"));
  shared_ptr_class_<OutgoingTensor> outgoingTensor(module, "OutgoingTensor");
  outgoingTensor.def(
      py::init<py::buffer, py::buffer>(),
      py::arg("buffer"),
      py::arg("metadata"));
  shared_ptr_class_<OutgoingMessage> outgoingMessage(module, "OutgoingMessage");
  outgoingMessage.def(
      py::init<
          py::buffer,
          const std::vector<std::shared_ptr<OutgoingPayload>>,
          const std::vector<std::shared_ptr<OutgoingTensor>>>(),
      py::arg("metadata"),
      py::arg("payloads"),
      py::arg("tensors"));

  shared_ptr_class_<IncomingPayload> incomingPayload(
      module, "IncomingPayload", py::buffer_protocol());
  incomingPayload.def_readonly("length", &IncomingPayload::length);
  incomingPayload.def_readonly("metadata", &IncomingPayload::metadata);
  incomingPayload.def_property(
      "buffer",
      [](IncomingPayload& pyPayload) -> py::buffer_info {
        TP_THROW_ASSERT_IF(!pyPayload.buffer.has_value()) << "No buffer";
        return pyPayload.buffer->getBuffer();
      },
      &IncomingPayload::set_buffer);
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
  incomingMessage.def_readonly("metadata", &IncomingMessage::metadata);
  incomingMessage.def_readonly("payloads", &IncomingMessage::payloads);
  incomingMessage.def_readonly("tensors", &IncomingMessage::tensors);

  // Creators.

  context.def(py::init<>());
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
                                 tensorpipe::Descriptor descriptor) mutable {
          if (error) {
            TP_LOG_ERROR() << error.what();
            return;
          }
          py::gil_scoped_acquire acquire;
          try {
            callback(prepareToAllocate(std::move(descriptor)));
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
        tensorpipe::Allocation tpAllocation =
            prepareToRead(std::move(pyMessage));
        pipe->read(
            std::move(tpAllocation),
            [callback{std::move(callback)}](
                const tensorpipe::Error& error) mutable {
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
                const tensorpipe::Error& error) mutable {
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

  shared_ptr_class_<tensorpipe::transport::Context> abstractTransport(
      module, "AbstractTransport");

  module.def("create_uv_transport", &tensorpipe::transport::uv::create);

#if TENSORPIPE_HAS_SHM_TRANSPORT
  module.def("create_shm_transport", &tensorpipe::transport::shm::create);
#endif // TENSORPIPE_HAS_SHM_TRANSPORT

  context.def(
      "register_transport",
      &tensorpipe::Context::registerTransport,
      py::arg("priority"),
      py::arg("name"),
      py::arg("transport"));

  shared_ptr_class_<tensorpipe::channel::Context> abstractChannel(
      module, "AbstractChannel");

  module.def("create_basic_channel", &tensorpipe::channel::basic::create);

#if TENSORPIPE_HAS_CMA_CHANNEL
  module.def("create_cma_channel", &tensorpipe::channel::cma::create);
#endif // TENSORPIPE_HAS_CMA_CHANNEL

  context.def(
      "register_channel",
      &tensorpipe::Context::registerChannel,
      py::arg("priority"),
      py::arg("name"),
      py::arg("channel"));

  // Helpers

  listener.def("get_url", &tensorpipe::Listener::url, py::arg("transport"));
}
