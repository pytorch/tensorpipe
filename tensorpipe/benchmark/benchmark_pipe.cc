/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>

#include <future>

#include <tensorpipe/benchmark/channel_registry.h>
#include <tensorpipe/benchmark/measurements.h>
#include <tensorpipe/benchmark/options.h>
#include <tensorpipe/benchmark/transport_registry.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>

using namespace tensorpipe;
using namespace tensorpipe::benchmark;

Measurements measurements;

struct Data {
  size_t numPayloads;
  size_t payloadSize;
  std::vector<std::unique_ptr<uint8_t[]>> expectedPayload;
  std::vector<std::string> expectedPayloadMetadata;
  std::vector<std::unique_ptr<uint8_t[]>> temporaryPayload;

  size_t numTensors;
  size_t tensorSize;
  std::vector<std::unique_ptr<uint8_t[]>> expectedTensor;
  std::vector<std::string> expectedTensorMetadata;
  std::vector<std::unique_ptr<uint8_t[]>> temporaryTensor;

  std::string expectedMetadata;
};

static void printMeasurements(Measurements& measurements, size_t dataLen) {
  measurements.sort();
  fprintf(
      stderr,
      "%-15s %-15s %-12s %-7s %-7s %-7s %-7s\n",
      "chunk-size",
      "# ping-pong",
      "avg (usec)",
      "p50",
      "p75",
      "p90",
      "p95");
  fprintf(
      stderr,
      "%-15lu %-15lu %-12.3f %-7.3f %-7.3f %-7.3f %-7.3f\n",
      dataLen,
      measurements.size(),
      measurements.sum().count() / (float)measurements.size() / 1000.0,
      measurements.percentile(0.50).count() / 1000.0,
      measurements.percentile(0.75).count() / 1000.0,
      measurements.percentile(0.90).count() / 1000.0,
      measurements.percentile(0.95).count() / 1000.0);
}

static std::unique_ptr<uint8_t[]> createData(const int size) {
  auto data = std::make_unique<uint8_t[]>(size);
  // Generate fixed data for validation between peers
  for (int i = 0; i < size; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  return data;
}

static void serverPongPingNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  pipe->readDescriptor([pipe, &numRoundTrips, &doneProm, &data, &measurements](
                           const Error& error, Message&& message) {
    TP_THROW_ASSERT_IF(error) << error.what();
    TP_DCHECK_EQ(message.metadata, data.expectedMetadata);
    if (data.payloadSize > 0) {
      TP_DCHECK_EQ(message.payloads.size(), data.numPayloads);
      for (size_t payloadIdx = 0; payloadIdx < data.numPayloads; payloadIdx++) {
        TP_DCHECK_EQ(
            message.payloads[payloadIdx].metadata,
            data.expectedPayloadMetadata[payloadIdx]);
        TP_DCHECK_EQ(message.payloads[payloadIdx].length, data.payloadSize);
        message.payloads[payloadIdx].data =
            data.temporaryPayload[payloadIdx].get();
      }
    } else {
      TP_DCHECK_EQ(message.payloads.size(), 0);
    }
    if (data.tensorSize > 0) {
      TP_DCHECK_EQ(message.tensors.size(), data.numTensors);
      for (size_t tensorIdx = 0; tensorIdx < data.numTensors; tensorIdx++) {
        TP_DCHECK_EQ(
            message.tensors[tensorIdx].metadata,
            data.expectedTensorMetadata[tensorIdx]);
        TP_DCHECK_EQ(
            message.tensors[tensorIdx].buffer.cpu.length, data.tensorSize);
        message.tensors[tensorIdx].buffer.cpu.ptr =
            data.temporaryTensor[tensorIdx].get();
      }
    } else {
      TP_DCHECK_EQ(message.tensors.size(), 0);
    }
    pipe->read(
        std::move(message),
        [pipe, &numRoundTrips, &doneProm, &data, &measurements](
            const Error& error, Message&& message) {
          TP_THROW_ASSERT_IF(error) << error.what();
          if (data.payloadSize > 0) {
            TP_DCHECK_EQ(message.payloads.size(), data.numPayloads);
            for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                 payloadIdx++) {
              TP_DCHECK_EQ(
                  message.payloads[payloadIdx].length, data.payloadSize);
              TP_DCHECK_EQ(
                  memcmp(
                      message.payloads[payloadIdx].data,
                      data.expectedPayload[payloadIdx].get(),
                      message.payloads[payloadIdx].length),
                  0);
            }
          } else {
            TP_DCHECK_EQ(message.tensors.size(), 0);
          }
          if (data.tensorSize > 0) {
            TP_DCHECK_EQ(message.tensors.size(), data.numTensors);
            for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                 tensorIdx++) {
              TP_DCHECK_EQ(
                  message.tensors[tensorIdx].buffer.cpu.length,
                  data.tensorSize);
              TP_DCHECK_EQ(
                  memcmp(
                      message.tensors[tensorIdx].buffer.cpu.ptr,
                      data.expectedTensor[tensorIdx].get(),
                      message.tensors[tensorIdx].buffer.cpu.length),
                  0);
            }
          } else {
            TP_DCHECK_EQ(message.tensors.size(), 0);
          }
          pipe->write(
              std::move(message),
              [pipe, &numRoundTrips, &doneProm, &data, &measurements](
                  const Error& error, Message&& message) {
                TP_THROW_ASSERT_IF(error) << error.what();
                if (--numRoundTrips > 0) {
                  serverPongPingNonBlock(
                      pipe, numRoundTrips, doneProm, data, measurements);
                } else {
                  doneProm.set_value();
                }
              });
        });
  });
}

// Start with receiving ping
static void runServer(const Options& options) {
  std::string addr = options.address;
  int numRoundTrips = options.numRoundTrips;

  Data data;
  data.numPayloads = options.numPayloads;
  data.payloadSize = options.payloadSize;
  for (size_t payloadIdx = 0; payloadIdx < options.numPayloads; payloadIdx++) {
    data.expectedPayload.push_back(createData(options.payloadSize));
    data.expectedPayloadMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryPayload.push_back(
        std::make_unique<uint8_t[]>(options.payloadSize));
  }
  data.numTensors = options.numTensors;
  data.tensorSize = options.tensorSize;
  for (size_t tensorIdx = 0; tensorIdx < options.numTensors; tensorIdx++) {
    data.expectedTensor.push_back(createData(options.tensorSize));
    data.expectedTensorMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryTensor.push_back(
        std::make_unique<uint8_t[]>(options.tensorSize));
  }
  data.expectedMetadata = std::string(options.metadataSize, 0x42);

  Measurements measurements;
  measurements.reserve(options.numRoundTrips);

  std::shared_ptr<Context> context = std::make_shared<Context>();
  auto transportContext =
      TensorpipeTransportRegistry().create(options.transport);
  validateTransportContext(transportContext);
  context->registerTransport(0, options.transport, transportContext);

  auto channelContext = TensorpipeChannelRegistry().create(options.channel);
  validateChannelContext(channelContext);
  context->registerChannel(0, options.channel, channelContext);

  std::promise<std::shared_ptr<Pipe>> pipeProm;
  std::shared_ptr<Listener> listener = context->listen({addr});
  listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
    TP_THROW_ASSERT_IF(error) << error.what();
    pipeProm.set_value(std::move(pipe));
  });
  std::shared_ptr<Pipe> pipe = pipeProm.get_future().get();

  std::promise<void> doneProm;
  serverPongPingNonBlock(
      std::move(pipe), numRoundTrips, doneProm, data, measurements);

  doneProm.get_future().get();
  listener.reset();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
  measurements.markStart();
  Message message;
  message.metadata = data.expectedMetadata;
  if (data.payloadSize > 0) {
    for (size_t payloadIdx = 0; payloadIdx < data.numPayloads; payloadIdx++) {
      Message::Payload payload;
      payload.data = data.expectedPayload[payloadIdx].get();
      payload.length = data.payloadSize;
      message.payloads.push_back(std::move(payload));
    }
  } else {
    TP_DCHECK_EQ(message.tensors.size(), 0);
  }
  if (data.tensorSize > 0) {
    for (size_t tensorIdx = 0; tensorIdx < data.numTensors; tensorIdx++) {
      Message::Tensor tensor;
      tensor.buffer =
          CpuBuffer{data.expectedTensor[tensorIdx].get(), data.tensorSize};
      message.tensors.push_back(std::move(tensor));
    }
  } else {
    TP_DCHECK_EQ(message.tensors.size(), 0);
  }
  pipe->write(
      std::move(message),
      [pipe, &numRoundTrips, &doneProm, &data, &measurements](
          const Error& error, Message&& message) {
        TP_THROW_ASSERT_IF(error) << error.what();
        pipe->readDescriptor(
            [pipe, &numRoundTrips, &doneProm, &data, &measurements](
                const Error& error, Message&& message) {
              TP_THROW_ASSERT_IF(error) << error.what();
              TP_DCHECK_EQ(message.metadata, data.expectedMetadata);
              if (data.payloadSize > 0) {
                TP_DCHECK_EQ(message.payloads.size(), data.numPayloads);
                for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                     payloadIdx++) {
                  TP_DCHECK_EQ(
                      message.payloads[payloadIdx].metadata,
                      data.expectedPayloadMetadata[payloadIdx]);
                  TP_DCHECK_EQ(
                      message.payloads[payloadIdx].length, data.payloadSize);
                  message.payloads[payloadIdx].data =
                      data.temporaryPayload[payloadIdx].get();
                }
              } else {
                TP_DCHECK_EQ(message.payloads.size(), 0);
              }
              if (data.tensorSize > 0) {
                TP_DCHECK_EQ(message.tensors.size(), data.numTensors);
                for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                     tensorIdx++) {
                  TP_DCHECK_EQ(
                      message.tensors[tensorIdx].metadata,
                      data.expectedTensorMetadata[tensorIdx]);
                  TP_DCHECK_EQ(
                      message.tensors[tensorIdx].buffer.cpu.length,
                      data.tensorSize);
                  message.tensors[tensorIdx].buffer.cpu.ptr =
                      data.temporaryTensor[tensorIdx].get();
                }
              } else {
                TP_DCHECK_EQ(message.tensors.size(), 0);
              }
              pipe->read(
                  std::move(message),
                  [pipe, &numRoundTrips, &doneProm, &data, &measurements](
                      const Error& error, Message&& message) {
                    measurements.markStop();
                    TP_THROW_ASSERT_IF(error) << error.what();
                    if (data.payloadSize > 0) {
                      TP_DCHECK_EQ(message.payloads.size(), data.numPayloads);
                      for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                           payloadIdx++) {
                        TP_DCHECK_EQ(
                            memcmp(
                                message.payloads[payloadIdx].data,
                                data.expectedPayload[payloadIdx].get(),
                                message.payloads[payloadIdx].length),
                            0);
                      }
                    } else {
                      TP_DCHECK_EQ(message.payloads.size(), 0);
                    }
                    if (data.tensorSize > 0) {
                      TP_DCHECK_EQ(message.tensors.size(), data.numTensors);
                      for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                           tensorIdx++) {
                        TP_DCHECK_EQ(
                            memcmp(
                                message.tensors[tensorIdx].buffer.cpu.ptr,
                                data.expectedTensor[tensorIdx].get(),
                                message.tensors[tensorIdx].buffer.cpu.length),
                            0);
                      }
                    } else {
                      TP_DCHECK_EQ(message.tensors.size(), 0);
                    }
                    if (--numRoundTrips > 0) {
                      clientPingPongNonBlock(
                          pipe, numRoundTrips, doneProm, data, measurements);
                    } else {
                      printMeasurements(measurements, data.payloadSize);
                      doneProm.set_value();
                    }
                  });
            });
      });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int numRoundTrips = options.numRoundTrips;

  Data data;
  data.numPayloads = options.numPayloads;
  data.payloadSize = options.payloadSize;
  for (size_t payloadIdx = 0; payloadIdx < options.numPayloads; payloadIdx++) {
    data.expectedPayload.push_back(createData(options.payloadSize));
    data.expectedPayloadMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryPayload.push_back(
        std::make_unique<uint8_t[]>(options.payloadSize));
  }
  data.numTensors = options.numTensors;
  data.tensorSize = options.tensorSize;
  for (size_t tensorIdx = 0; tensorIdx < options.numTensors; tensorIdx++) {
    data.expectedTensor.push_back(createData(options.tensorSize));
    data.expectedTensorMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryTensor.push_back(
        std::make_unique<uint8_t[]>(options.tensorSize));
  }
  data.expectedMetadata = std::string(options.metadataSize, 0x42);

  Measurements measurements;
  measurements.reserve(options.numRoundTrips);

  std::shared_ptr<Context> context = std::make_shared<Context>();
  auto transportContext =
      TensorpipeTransportRegistry().create(options.transport);
  validateTransportContext(transportContext);
  context->registerTransport(0, options.transport, transportContext);

  auto channelContext = TensorpipeChannelRegistry().create(options.channel);
  validateChannelContext(channelContext);
  context->registerChannel(0, options.channel, channelContext);

  std::shared_ptr<Pipe> pipe = context->connect(addr);

  std::promise<void> doneProm;
  clientPingPongNonBlock(
      std::move(pipe), numRoundTrips, doneProm, data, measurements);

  doneProm.get_future().get();
  context->join();
}

int main(int argc, char** argv) {
  struct Options x = parseOptions(argc, argv);
  std::cout << "mode = " << x.mode << "\n";
  std::cout << "transport = " << x.transport << "\n";
  std::cout << "channel = " << x.channel << "\n";
  std::cout << "address = " << x.address << "\n";
  std::cout << "num_round_trips = " << x.numRoundTrips << "\n";
  std::cout << "num_payloads = " << x.numPayloads << "\n";
  std::cout << "payload_size = " << x.payloadSize << "\n";
  std::cout << "num_tensors = " << x.numTensors << "\n";
  std::cout << "tensor_size = " << x.tensorSize << "\n";
  std::cout << "metadata_size = " << x.metadataSize << "\n";

  if (x.mode == "listen") {
    runServer(x);
  } else if (x.mode == "connect") {
    runClient(x);
  } else {
    // Should never be here
    TP_THROW_ASSERT() << "unknown mode: " << x.mode;
  }

  return 0;
}
