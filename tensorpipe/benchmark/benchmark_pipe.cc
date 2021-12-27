/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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
#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/common/cuda_buffer.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/core/context.h>
#include <tensorpipe/core/listener.h>
#include <tensorpipe/core/pipe.h>

// We might sometimes want to run this benchmark using NCCL instead of
// TensorPipe. We don't want to include NCCL as a submodule and deal with the
// build issues. So we've prepared the code and left it around, but disabled it.
#if USE_NCCL
#include <nccl.h>

#define TP_NCCL_CHECK(op)                   \
  {                                         \
    ncclResult_t res = (op);                \
    TP_THROW_ASSERT_IF(res != ncclSuccess); \
  }

struct NcclCommDeleter {
  void operator()(ncclComm_t comm) {
    TP_NCCL_CHECK(ncclCommDestroy(comm));
  }
};

using NcclComm =
    std::unique_ptr<std::remove_pointer_t<ncclComm_t>, NcclCommDeleter>;

static NcclComm createNcclComm(int rank, int worldSize, ncclUniqueId uniqueId) {
  ncclComm_t comm;
  TP_NCCL_CHECK(ncclCommInitRank(&comm, worldSize, uniqueId, rank));
  return NcclComm(comm, NcclCommDeleter{});
}
#endif // USE_NCCL

using namespace tensorpipe;
using namespace tensorpipe::benchmark;

static constexpr int kNumWarmUpRounds = 5;

using Payload = std::unique_ptr<uint8_t[]>;
using CpuTensor = std::unique_ptr<uint8_t[]>;

struct CudaMemoryDeleter {
  void operator()(void* ptr) {
    TP_CUDA_CHECK(cudaFree(ptr));
  }
};

struct CudaStreamDeleter {
  void operator()(cudaStream_t stream) {
    TP_CUDA_CHECK(cudaStreamDestroy(stream));
  }
};

using CudaTensor = std::unique_ptr<uint8_t[], CudaMemoryDeleter>;
using CudaStream =
    std::unique_ptr<std::remove_pointer_t<cudaStream_t>, CudaStreamDeleter>;

struct Data {
  size_t numPayloads;
  size_t payloadSize;
  std::vector<Payload> expectedPayload;
  std::vector<std::string> expectedPayloadMetadata;
  std::vector<Payload> temporaryPayload;

  size_t numTensors;
  size_t tensorSize;
  TensorType tensorType;
  std::vector<CpuTensor> expectedCpuTensor;
  std::vector<CudaTensor> expectedCudaTensor;
  std::vector<std::string> expectedTensorMetadata;
  std::vector<CpuTensor> temporaryCpuTensor;
  std::vector<CudaTensor> temporaryCudaTensor;
  CudaStream cudaStream;
  size_t cudaSyncPeriod;

  std::string expectedMetadata;

#if USE_NCCL
  NcclComm ncclComm;
#endif // USE_NCCL
};

struct MultiDeviceMeasurements {
  // The CPU time to do each ping-pong.
  Measurements cpu;
  // The CPU time of N iterations, including a final CUDA stream sync.
  Measurements cuda;
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

static void printMultiDeviceMeasurements(
    MultiDeviceMeasurements& measurements,
    size_t dataLen) {
  printMeasurements(measurements.cpu, dataLen);
  printMeasurements(measurements.cuda, dataLen);
}

static std::unique_ptr<uint8_t[]> createEmptyCpuData(size_t size) {
  return std::make_unique<uint8_t[]>(size);
}

static std::unique_ptr<uint8_t[]> createFullCpuData(size_t size) {
  std::unique_ptr<uint8_t[]> data = createEmptyCpuData(size);
  // Generate fixed data for validation between peers
  for (size_t i = 0; i < size; i++) {
    data[i] = (i >> 8) ^ (i & 0xff);
  }
  return data;
}

static CudaTensor createEmptyCudaData(size_t size) {
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMalloc(&ptr, size));
  return CudaTensor(ptr);
}

static CudaTensor createFullCudaData(size_t size) {
  uint8_t* ptr;
  TP_CUDA_CHECK(cudaMalloc(&ptr, size));
  CpuTensor data = createFullCpuData(size);
  TP_CUDA_CHECK(cudaMemcpy(ptr, data.get(), size, cudaMemcpyHostToDevice));
  return CudaTensor(ptr);
}

static CudaStream createCudaStream() {
  cudaStream_t stream;
  TP_CUDA_CHECK(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking));
  return CudaStream(stream);
}

static void serverPongPingNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& numWarmUps,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    Measurements& measurements) {
#if USE_NCCL
  for (int iterIdx = 0; iterIdx < numWarmUps + numRoundTrips; iterIdx++) {
    // TODO Handle multiple tensors.
    TP_NCCL_CHECK(ncclRecv(
        data.temporaryCudaTensor[0].get(),
        data.tensorSize,
        ncclInt8,
        1,
        data.ncclComm.get(),
        data.cudaStream.get()));
    TP_NCCL_CHECK(ncclSend(
        data.temporaryCudaTensor[0].get(),
        data.tensorSize,
        ncclInt8,
        1,
        data.ncclComm.get(),
        data.cudaStream.get()));
  }
  doneProm.set_value();
  return;
#endif // USE_NCCL
  pipe->readDescriptor(
      [pipe, &numWarmUps, &numRoundTrips, &doneProm, &data, &measurements](
          const Error& error, Descriptor descriptor) {
        TP_THROW_ASSERT_IF(error) << error.what();
        Allocation allocation;
        TP_DCHECK_EQ(descriptor.metadata, data.expectedMetadata);
        if (data.payloadSize > 0) {
          TP_DCHECK_EQ(descriptor.payloads.size(), data.numPayloads);
          allocation.payloads.resize(data.numPayloads);
          for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
               payloadIdx++) {
            TP_DCHECK_EQ(
                descriptor.payloads[payloadIdx].metadata,
                data.expectedPayloadMetadata[payloadIdx]);
            TP_DCHECK_EQ(
                descriptor.payloads[payloadIdx].length, data.payloadSize);
            allocation.payloads[payloadIdx].data =
                data.temporaryPayload[payloadIdx].get();
          }
        } else {
          TP_DCHECK_EQ(descriptor.payloads.size(), 0);
        }
        if (data.tensorSize > 0) {
          TP_DCHECK_EQ(descriptor.tensors.size(), data.numTensors);
          allocation.tensors.resize(data.numTensors);
          for (size_t tensorIdx = 0; tensorIdx < data.numTensors; tensorIdx++) {
            TP_DCHECK_EQ(
                descriptor.tensors[tensorIdx].metadata,
                data.expectedTensorMetadata[tensorIdx]);
            TP_DCHECK_EQ(descriptor.tensors[tensorIdx].length, data.tensorSize);
            if (data.tensorType == TensorType::kCpu) {
              allocation.tensors[tensorIdx].buffer = CpuBuffer{
                  .ptr = data.temporaryCpuTensor[tensorIdx].get(),
              };
            } else if (data.tensorType == TensorType::kCuda) {
              allocation.tensors[tensorIdx].buffer = CudaBuffer{
                  .ptr = data.temporaryCudaTensor[tensorIdx].get(),
                  .stream = data.cudaStream.get(),
              };
            } else {
              TP_THROW_ASSERT() << "Unknown tensor type";
            }
          }
        } else {
          TP_DCHECK_EQ(descriptor.tensors.size(), 0);
        }

        pipe->read(
            allocation,
            [pipe,
             &numWarmUps,
             &numRoundTrips,
             &doneProm,
             &data,
             &measurements,
             descriptor{std::move(descriptor)},
             allocation](const Error& error) {
              TP_THROW_ASSERT_IF(error) << error.what();

              Message message;
              if (data.payloadSize > 0) {
                TP_DCHECK_EQ(allocation.payloads.size(), data.numPayloads);
                message.payloads.resize(data.numPayloads);
                for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                     payloadIdx++) {
                  TP_DCHECK_EQ(
                      descriptor.payloads[payloadIdx].length, data.payloadSize);
                  TP_DCHECK_EQ(
                      memcmp(
                          allocation.payloads[payloadIdx].data,
                          data.expectedPayload[payloadIdx].get(),
                          descriptor.payloads[payloadIdx].length),
                      0);
                  message.payloads[payloadIdx] = {
                      .data = data.expectedPayload[payloadIdx].get(),
                      .length = descriptor.payloads[payloadIdx].length,
                  };
                }
              } else {
                TP_DCHECK_EQ(allocation.payloads.size(), 0);
              }
              if (data.tensorSize > 0) {
                TP_DCHECK_EQ(allocation.tensors.size(), data.numTensors);
                message.tensors.resize(data.numTensors);
                for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                     tensorIdx++) {
                  TP_DCHECK_EQ(
                      descriptor.tensors[tensorIdx].length, data.tensorSize);
                  if (data.tensorType == TensorType::kCpu) {
                    TP_DCHECK_EQ(
                        memcmp(
                            allocation.tensors[tensorIdx]
                                .buffer.unwrap<CpuBuffer>()
                                .ptr,
                            data.expectedCpuTensor[tensorIdx].get(),
                            descriptor.tensors[tensorIdx].length),
                        0);
                  } else if (data.tensorType == TensorType::kCuda) {
                    // No (easy) way to do a memcmp with CUDA, I believe...
                  } else {
                    TP_THROW_ASSERT() << "Unknown tensor type";
                  }
                  message.tensors[tensorIdx] = {
                      .buffer = allocation.tensors[tensorIdx].buffer,
                      .length = descriptor.tensors[tensorIdx].length,
                      .targetDevice =
                          descriptor.tensors[tensorIdx].sourceDevice,
                  };
                }
              } else {
                TP_DCHECK_EQ(allocation.tensors.size(), 0);
              }

              pipe->write(
                  std::move(message),
                  [pipe,
                   &numWarmUps,
                   &numRoundTrips,
                   &doneProm,
                   &data,
                   &measurements](const Error& error) {
                    TP_THROW_ASSERT_IF(error) << error.what();
                    if (numWarmUps > 0) {
                      numWarmUps -= 1;
                    } else {
                      numRoundTrips -= 1;
                    }
                    if (numRoundTrips > 0) {
                      serverPongPingNonBlock(
                          pipe,
                          numWarmUps,
                          numRoundTrips,
                          doneProm,
                          data,
                          measurements);
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
  int numWarmUps = kNumWarmUpRounds;
  int numRoundTrips = options.numRoundTrips;

  Data data;
  data.numPayloads = options.numPayloads;
  data.payloadSize = options.payloadSize;
  for (size_t payloadIdx = 0; payloadIdx < options.numPayloads; payloadIdx++) {
    data.expectedPayload.push_back(createFullCpuData(options.payloadSize));
    data.expectedPayloadMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryPayload.push_back(createEmptyCpuData(options.payloadSize));
  }
  data.numTensors = options.numTensors;
  data.tensorSize = options.tensorSize;
  data.tensorType = options.tensorType;
  for (size_t tensorIdx = 0; tensorIdx < options.numTensors; tensorIdx++) {
    data.expectedTensorMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    if (options.tensorType == TensorType::kCpu) {
      data.expectedCpuTensor.push_back(createFullCpuData(options.tensorSize));
      data.temporaryCpuTensor.push_back(createEmptyCpuData(options.tensorSize));
    } else if (options.tensorType == TensorType::kCuda) {
      data.expectedCudaTensor.push_back(createFullCudaData(options.tensorSize));
      data.temporaryCudaTensor.push_back(
          createEmptyCudaData(options.tensorSize));
      data.cudaStream = createCudaStream();
    } else {
      TP_THROW_ASSERT() << "Unknown tensor type";
    }
  }
  data.cudaSyncPeriod = options.cudaSyncPeriod;
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

#if USE_NCCL
  std::promise<ncclUniqueId> uniqueIdProm;
  pipe->readDescriptor([&](const Error& error, Descriptor descriptor) {
    TP_THROW_ASSERT_IF(error) << error.what();
    uniqueIdProm.set_value(
        *reinterpret_cast<const ncclUniqueId*>(descriptor.metadata.c_str()));
  });
  ncclUniqueId uniqueId = uniqueIdProm.get_future().get();

  data.ncclComm = createNcclComm(/*rank=*/0, /*worldSize=*/2, uniqueId);
#endif

  std::promise<void> doneProm;
  serverPongPingNonBlock(
      std::move(pipe), numWarmUps, numRoundTrips, doneProm, data, measurements);

  doneProm.get_future().get();
  listener.reset();
  context->join();
}

static void clientPingPongNonBlock(
    std::shared_ptr<Pipe> pipe,
    int& numWarmUps,
    int& numRoundTrips,
    std::promise<void>& doneProm,
    Data& data,
    MultiDeviceMeasurements& measurements) {
#if USE_NCCL
  for (int iterIdx = 0; iterIdx < numWarmUps + numRoundTrips; iterIdx++) {
    if (iterIdx >= numWarmUps) {
      measurements.cpu.markStart();
      if ((iterIdx - numWarmUps) % data.cudaSyncPeriod == 0) {
        measurements.cuda.markStart();
      }
    }
    TP_NCCL_CHECK(ncclSend(
        data.expectedCudaTensor[0].get(),
        data.tensorSize,
        ncclInt8,
        0,
        data.ncclComm.get(),
        data.cudaStream.get()));
    TP_NCCL_CHECK(ncclRecv(
        data.temporaryCudaTensor[0].get(),
        data.tensorSize,
        ncclInt8,
        0,
        data.ncclComm.get(),
        data.cudaStream.get()));
    if (iterIdx >= numWarmUps) {
      measurements.cpu.markStop();
      if ((iterIdx - numWarmUps + 1) % data.cudaSyncPeriod == 0) {
        TP_CUDA_CHECK(cudaStreamSynchronize(data.cudaStream.get()));
        measurements.cuda.markStop(data.cudaSyncPeriod);
      }
    }
  }
  printMultiDeviceMeasurements(measurements, data.payloadSize);
  doneProm.set_value();
  return;
#endif // USE_NCCL
  if (numWarmUps == 0) {
    measurements.cpu.markStart();
    if (numRoundTrips % data.cudaSyncPeriod == 0) {
      measurements.cuda.markStart();
    }
  }
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
    TP_DCHECK_EQ(message.payloads.size(), 0);
  }
  if (data.tensorSize > 0) {
    for (size_t tensorIdx = 0; tensorIdx < data.numTensors; tensorIdx++) {
      Message::Tensor tensor;
      tensor.length = data.tensorSize;
      if (data.tensorType == TensorType::kCpu) {
        tensor.buffer =
            CpuBuffer{.ptr = data.expectedCpuTensor[tensorIdx].get()};
        tensor.targetDevice = Device(kCpuDeviceType, 0);
      } else if (data.tensorType == TensorType::kCuda) {
        tensor.buffer = CudaBuffer{
            .ptr = data.expectedCudaTensor[tensorIdx].get(),
            .stream = data.cudaStream.get(),
        };
        tensor.targetDevice = Device(kCudaDeviceType, 0);
      } else {
        TP_THROW_ASSERT() << "Unknown tensor type";
      }
      message.tensors.push_back(std::move(tensor));
    }
  } else {
    TP_DCHECK_EQ(message.tensors.size(), 0);
  }
  pipe->write(
      std::move(message),
      [pipe, &numWarmUps, &numRoundTrips, &doneProm, &data, &measurements](
          const Error& error) {
        TP_THROW_ASSERT_IF(error) << error.what();
        pipe->readDescriptor([pipe,
                              &numWarmUps,
                              &numRoundTrips,
                              &doneProm,
                              &data,
                              &measurements](
                                 const Error& error, Descriptor descriptor) {
          TP_THROW_ASSERT_IF(error) << error.what();

          Allocation allocation;
          TP_DCHECK_EQ(descriptor.metadata, data.expectedMetadata);
          if (data.payloadSize > 0) {
            TP_DCHECK_EQ(descriptor.payloads.size(), data.numPayloads);
            allocation.payloads.resize(data.numPayloads);
            for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                 payloadIdx++) {
              TP_DCHECK_EQ(
                  descriptor.payloads[payloadIdx].metadata,
                  data.expectedPayloadMetadata[payloadIdx]);
              TP_DCHECK_EQ(
                  descriptor.payloads[payloadIdx].length, data.payloadSize);
              allocation.payloads[payloadIdx].data =
                  data.temporaryPayload[payloadIdx].get();
            }
          } else {
            TP_DCHECK_EQ(descriptor.payloads.size(), 0);
          }
          if (data.tensorSize > 0) {
            TP_DCHECK_EQ(descriptor.tensors.size(), data.numTensors);
            allocation.tensors.resize(data.numTensors);
            for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                 tensorIdx++) {
              TP_DCHECK_EQ(
                  descriptor.tensors[tensorIdx].metadata,
                  data.expectedTensorMetadata[tensorIdx]);
              TP_DCHECK_EQ(
                  descriptor.tensors[tensorIdx].length, data.tensorSize);
              if (data.tensorType == TensorType::kCpu) {
                allocation.tensors[tensorIdx].buffer = CpuBuffer{
                    .ptr = data.temporaryCpuTensor[tensorIdx].get(),
                };
              } else if (data.tensorType == TensorType::kCuda) {
                allocation.tensors[tensorIdx].buffer = CudaBuffer{
                    .ptr = data.temporaryCudaTensor[tensorIdx].get(),
                    .stream = data.cudaStream.get(),
                };
              } else {
                TP_THROW_ASSERT() << "Unknown tensor type";
              }
            }
          } else {
            TP_DCHECK_EQ(descriptor.tensors.size(), 0);
          }
          pipe->read(
              allocation,
              [pipe,
               &numWarmUps,
               &numRoundTrips,
               &doneProm,
               &data,
               &measurements,
               descriptor{std::move(descriptor)},
               allocation](const Error& error) {
                if (numWarmUps == 0) {
                  measurements.cpu.markStop();
                  if ((numRoundTrips - 1) % data.cudaSyncPeriod == 0) {
                    TP_CUDA_CHECK(cudaStreamSynchronize(data.cudaStream.get()));
                    measurements.cuda.markStop(data.cudaSyncPeriod);
                  }
                }
                TP_THROW_ASSERT_IF(error) << error.what();
                if (data.payloadSize > 0) {
                  TP_DCHECK_EQ(allocation.payloads.size(), data.numPayloads);
                  for (size_t payloadIdx = 0; payloadIdx < data.numPayloads;
                       payloadIdx++) {
                    TP_DCHECK_EQ(
                        memcmp(
                            allocation.payloads[payloadIdx].data,
                            data.expectedPayload[payloadIdx].get(),
                            descriptor.payloads[payloadIdx].length),
                        0);
                  }
                } else {
                  TP_DCHECK_EQ(allocation.payloads.size(), 0);
                }
                if (data.tensorSize > 0) {
                  TP_DCHECK_EQ(allocation.tensors.size(), data.numTensors);
                  for (size_t tensorIdx = 0; tensorIdx < data.numTensors;
                       tensorIdx++) {
                    if (data.tensorType == TensorType::kCpu) {
                      TP_DCHECK_EQ(
                          memcmp(
                              allocation.tensors[tensorIdx]
                                  .buffer.unwrap<CpuBuffer>()
                                  .ptr,
                              data.expectedCpuTensor[tensorIdx].get(),
                              descriptor.tensors[tensorIdx].length),
                          0);
                    } else if (data.tensorType == TensorType::kCuda) {
                      // No (easy) way to do a memcmp with CUDA, I
                      // believe...
                    } else {
                      TP_THROW_ASSERT() << "Unknown tensor type";
                    }
                  }
                } else {
                  TP_DCHECK_EQ(allocation.tensors.size(), 0);
                }
                if (numWarmUps > 0) {
                  numWarmUps -= 1;
                } else {
                  numRoundTrips -= 1;
                }
                if (numRoundTrips > 0) {
                  clientPingPongNonBlock(
                      pipe,
                      numWarmUps,
                      numRoundTrips,
                      doneProm,
                      data,
                      measurements);
                } else {
                  printMultiDeviceMeasurements(measurements, data.payloadSize);
                  doneProm.set_value();
                }
              });
        });
      });
}

// Start with sending ping
static void runClient(const Options& options) {
  std::string addr = options.address;
  int numWarmUps = kNumWarmUpRounds;
  int numRoundTrips = options.numRoundTrips;

  Data data;
  data.numPayloads = options.numPayloads;
  data.payloadSize = options.payloadSize;
  for (size_t payloadIdx = 0; payloadIdx < options.numPayloads; payloadIdx++) {
    data.expectedPayload.push_back(createFullCpuData(options.payloadSize));
    data.expectedPayloadMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    data.temporaryPayload.push_back(createEmptyCpuData(options.payloadSize));
  }
  data.numTensors = options.numTensors;
  data.tensorSize = options.tensorSize;
  data.tensorType = options.tensorType;
  for (size_t tensorIdx = 0; tensorIdx < options.numTensors; tensorIdx++) {
    data.expectedTensorMetadata.push_back(
        std::string(options.metadataSize, 0x42));
    if (data.tensorType == TensorType::kCpu) {
      data.expectedCpuTensor.push_back(createFullCpuData(options.tensorSize));
      data.temporaryCpuTensor.push_back(createEmptyCpuData(options.tensorSize));
    } else if (data.tensorType == TensorType::kCuda) {
      data.expectedCudaTensor.push_back(createFullCudaData(options.tensorSize));
      data.temporaryCudaTensor.push_back(
          createEmptyCudaData(options.tensorSize));
      data.cudaStream = createCudaStream();
    } else {
      TP_THROW_ASSERT() << "Unknown tensor type";
    }
  }
  data.cudaSyncPeriod = options.cudaSyncPeriod;
  data.expectedMetadata = std::string(options.metadataSize, 0x42);

  MultiDeviceMeasurements measurements;
  measurements.cpu.reserve(options.numRoundTrips);
  measurements.cuda.reserve(options.numRoundTrips / data.cudaSyncPeriod);

  std::shared_ptr<Context> context = std::make_shared<Context>();
  auto transportContext =
      TensorpipeTransportRegistry().create(options.transport);
  validateTransportContext(transportContext);
  context->registerTransport(0, options.transport, transportContext);

  auto channelContext = TensorpipeChannelRegistry().create(options.channel);
  validateChannelContext(channelContext);
  context->registerChannel(0, options.channel, channelContext);

  std::shared_ptr<Pipe> pipe = context->connect(addr);

#if USE_NCCL
  ncclUniqueId uniqueId;
  TP_NCCL_CHECK(ncclGetUniqueId(&uniqueId));
  Message message;
  message.metadata = std::string(
      reinterpret_cast<char*>(&uniqueId),
      reinterpret_cast<char*>(&uniqueId) + sizeof(ncclUniqueId));
  std::promise<void> promise;
  pipe->write(std::move(message), [&](const Error& error) {
    TP_THROW_ASSERT_IF(error) << error.what();
    promise.set_value();
  });
  promise.get_future().get();

  data.ncclComm = createNcclComm(/*rank=*/1, /*worldSize=*/2, uniqueId);
#endif // USE_NCCL

  std::promise<void> doneProm;
  clientPingPongNonBlock(
      std::move(pipe), numWarmUps, numRoundTrips, doneProm, data, measurements);

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
  std::cout << "tensor_type = "
            << (x.tensorType == TensorType::kCpu ? "cpu" : "cuda") << "\n";
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
