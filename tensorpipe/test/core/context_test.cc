/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <tensorpipe/tensorpipe.h>
#include <tensorpipe/test/peer_group.h>

#if TP_USE_CUDA
#include <tensorpipe/common/cuda.h>
#include <tensorpipe/tensorpipe_cuda.h>
#endif // TP_USE_CUDA

using namespace tensorpipe;

namespace {

::testing::AssertionResult buffersAreEqual(
    const void* ptr1,
    const size_t len1,
    const void* ptr2,
    const size_t len2) {
  if (ptr1 == nullptr && ptr2 == nullptr) {
    if (len1 == 0 && len2 == 0) {
      return ::testing::AssertionSuccess();
    }
    if (len1 != 0) {
      return ::testing::AssertionFailure()
          << "first pointer is null but length isn't 0";
    }
    if (len1 != 0) {
      return ::testing::AssertionFailure()
          << "second pointer is null but length isn't 0";
    }
  }
  if (ptr1 == nullptr) {
    return ::testing::AssertionFailure()
        << "first pointer is null but second one isn't";
  }
  if (ptr2 == nullptr) {
    return ::testing::AssertionFailure()
        << "second pointer is null but first one isn't";
  }
  if (len1 != len2) {
    return ::testing::AssertionFailure()
        << "first length is " << len1 << " but second one is " << len2;
  }
  if (std::memcmp(ptr1, ptr2, len1) != 0) {
    return ::testing::AssertionFailure() << "buffer contents aren't equal";
  }
  return ::testing::AssertionSuccess();
}

#if TP_USE_CUDA
std::vector<uint8_t> unwrapCudaBuffer(CudaBuffer b, size_t length) {
  std::vector<uint8_t> result(length);
  TP_CUDA_CHECK(cudaStreamSynchronize(b.stream));
  TP_CUDA_CHECK(cudaMemcpy(result.data(), b.ptr, length, cudaMemcpyDefault));

  return result;
}
#endif // TP_USE_CUDA

::testing::AssertionResult descriptorAndAllocationMatchMessage(
    const Descriptor& descriptor,
    const Allocation& allocation,
    const Message& message) {
  EXPECT_EQ(descriptor.payloads.size(), allocation.payloads.size());
  if (descriptor.payloads.size() != message.payloads.size()) {
    return ::testing::AssertionFailure()
        << "descriptor has " << descriptor.payloads.size()
        << " payloads but message has " << message.payloads.size();
  }
  for (size_t idx = 0; idx < descriptor.payloads.size(); idx++) {
    EXPECT_TRUE(buffersAreEqual(
        allocation.payloads[idx].data,
        descriptor.payloads[idx].length,
        message.payloads[idx].data,
        message.payloads[idx].length));
  }
  EXPECT_EQ(descriptor.tensors.size(), allocation.tensors.size());
  if (descriptor.tensors.size() != message.tensors.size()) {
    return ::testing::AssertionFailure()
        << "descriptor has " << descriptor.tensors.size()
        << " tensors but message has " << message.tensors.size();
  }
  for (size_t idx = 0; idx < descriptor.tensors.size(); idx++) {
    EXPECT_EQ(
        allocation.tensors[idx].buffer.device(),
        message.tensors[idx].buffer.device());
    const std::string& deviceType =
        allocation.tensors[idx].buffer.device().type;

    if (deviceType == kCpuDeviceType) {
      EXPECT_TRUE(buffersAreEqual(
          allocation.tensors[idx].buffer.unwrap<CpuBuffer>().ptr,
          descriptor.tensors[idx].length,
          message.tensors[idx].buffer.unwrap<CpuBuffer>().ptr,
          message.tensors[idx].length));
#if TP_USE_CUDA
    } else if (deviceType == kCudaDeviceType) {
      std::vector<uint8_t> buffer1 = unwrapCudaBuffer(
          allocation.tensors[idx].buffer.unwrap<CudaBuffer>(),
          descriptor.tensors[idx].length);
      std::vector<uint8_t> buffer2 = unwrapCudaBuffer(
          message.tensors[idx].buffer.unwrap<CudaBuffer>(),
          message.tensors[idx].length);
      EXPECT_TRUE(buffersAreEqual(
          buffer1.data(), buffer1.size(), buffer2.data(), buffer2.size()));
#endif // TP_USE_CUDA
    } else {
      ADD_FAILURE() << "Unexpected device type: " << deviceType;
    }
  }
  return ::testing::AssertionSuccess();
}

#if TP_USE_CUDA
struct CudaPointerDeleter {
  void operator()(void* ptr) {
    TP_CUDA_CHECK(cudaFree(ptr));
  }
};

std::unique_ptr<void, CudaPointerDeleter> makeCudaPointer(size_t length) {
  void* cudaPtr;
  TP_CUDA_CHECK(cudaMalloc(&cudaPtr, length));
  return std::unique_ptr<void, CudaPointerDeleter>(cudaPtr);
}
#endif // TP_USE_CUDA

// Having 4 payloads per message is arbitrary.
constexpr int kNumPayloads = 4;
// Having 4 tensors per message ensures there are 2 CPU tensors and 2 CUDA
// tensors.
constexpr int kNumTensors = 4;
std::string kPayloadData = "I'm a payload";
std::string kTensorData = "And I'm a tensor";
#if TP_USE_CUDA
const int kCudaTensorLength = 32;
const uint8_t kCudaTensorFillValue = 0x42;
#endif // TP_USE_CUDA

Message::Tensor makeTensor(int index) {
#if TP_USE_CUDA
  static std::unique_ptr<void, CudaPointerDeleter> kCudaTensorData = []() {
    auto cudaPtr = makeCudaPointer(kCudaTensorLength);
    TP_CUDA_CHECK(
        cudaMemset(cudaPtr.get(), kCudaTensorFillValue, kCudaTensorLength));
    return cudaPtr;
  }();

  if (index % 2 == 1) {
    return {
        .buffer =
            CudaBuffer{
                .ptr = kCudaTensorData.get(),
                .stream = cudaStreamDefault,
            },
        // FIXME: Use non-blocking stream.
        .length = kCudaTensorLength,
    };
  }
#endif // TP_USE_CUDA

  return {
      .buffer =
          CpuBuffer{
              .ptr = reinterpret_cast<void*>(
                  const_cast<char*>(kTensorData.data())),
          },
      .length = kTensorData.length(),
  };
}

Message makeMessage(int numPayloads, int numTensors) {
  Message message;
  for (int i = 0; i < numPayloads; i++) {
    Message::Payload payload;
    payload.data =
        reinterpret_cast<void*>(const_cast<char*>(kPayloadData.data()));
    payload.length = kPayloadData.length();
    message.payloads.push_back(std::move(payload));
  }
  for (int i = 0; i < numTensors; i++) {
    message.tensors.push_back(makeTensor(i));
  }
  return message;
}

Allocation allocateForDescriptor(
    const Descriptor& descriptor,
    std::vector<std::shared_ptr<void>>& buffers) {
  Allocation allocation;
  for (const auto& payload : descriptor.payloads) {
    // FIXME: Changing this to a make_shared causes havoc.
    auto payloadData = std::unique_ptr<uint8_t, std::default_delete<uint8_t[]>>(
        new uint8_t[payload.length]);
    allocation.payloads.push_back({.data = payloadData.get()});
    buffers.push_back(std::move(payloadData));
  }
  for (const auto& tensor : descriptor.tensors) {
    if (tensor.sourceDevice.type == kCpuDeviceType) {
      auto tensorData =
          std::unique_ptr<uint8_t, std::default_delete<uint8_t[]>>(
              new uint8_t[tensor.length]);
      allocation.tensors.push_back({
          .buffer = CpuBuffer{.ptr = tensorData.get()},
      });
      buffers.push_back(std::move(tensorData));
#if TP_USE_CUDA
    } else if (tensor.sourceDevice.type == kCudaDeviceType) {
      auto tensorData = makeCudaPointer(tensor.length);
      allocation.tensors.push_back({
          .buffer =
              CudaBuffer{
                  .ptr = tensorData.get(),
                  // FIXME: Use non-blocking streams.
                  .stream = cudaStreamDefault,
              },
      });
      buffers.push_back(std::move(tensorData));
#endif // TP_USE_CUDA
    } else {
      ADD_FAILURE() << "Unrecognized device type: " << tensor.sourceDevice.type;
    }
  }

  return allocation;
}

Message messageFromAllocation(
    const Descriptor& descriptor,
    const Allocation& allocation) {
  Message message;
  message.metadata = descriptor.metadata;
  for (int payloadIdx = 0; payloadIdx < descriptor.payloads.size();
       ++payloadIdx) {
    message.payloads.emplace_back();
    Message::Payload& payload = message.payloads.back();
    payload.metadata = descriptor.payloads[payloadIdx].metadata;
    payload.length = descriptor.payloads[payloadIdx].length;
    payload.data = allocation.payloads[payloadIdx].data;
  }
  for (int tensorIdx = 0; tensorIdx < descriptor.tensors.size(); ++tensorIdx) {
    message.tensors.emplace_back();
    Message::Tensor& tensor = message.tensors.back();
    tensor.metadata = descriptor.tensors[tensorIdx].metadata;
    tensor.length = descriptor.tensors[tensorIdx].length;
    tensor.buffer = allocation.tensors[tensorIdx].buffer;
  }

  return message;
}

std::vector<std::string> genUrls() {
  std::vector<std::string> res;

#if TENSORPIPE_HAS_SHM_TRANSPORT
  res.push_back("shm://");
#endif // TENSORPIPE_HAS_SHM_TRANSPORT
  res.push_back("uv://127.0.0.1");

  return res;
}

std::shared_ptr<Context> makeContext() {
  auto context = std::make_shared<Context>();

  context->registerTransport(0, "uv", transport::uv::create());
#if TENSORPIPE_HAS_SHM_TRANSPORT
  context->registerTransport(1, "shm", transport::shm::create());
#endif // TENSORPIPE_HAS_SHM_TRANSPORT
  context->registerChannel(0, "basic", channel::basic::create());
#if TENSORPIPE_HAS_CMA_CHANNEL
  context->registerChannel(1, "cma", channel::cma::create());
#endif // TENSORPIPE_HAS_CMA_CHANNEL
#if TP_USE_CUDA
  context->registerChannel(
      10, "cuda_basic", channel::cuda_basic::create(channel::basic::create()));
#if TENSORPIPE_HAS_CUDA_IPC_CHANNEL
  context->registerChannel(11, "cuda_ipc", channel::cuda_ipc::create());
#endif // TENSORPIPE_HAS_CUDA_IPC_CHANNEL
  context->registerChannel(12, "cuda_xth", channel::cuda_xth::create());
#endif // TP_USE_CUDA

  return context;
}

} // namespace

TEST(Context, ClientPingSerial) {
  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        std::vector<std::shared_ptr<void>> buffers;
        std::promise<std::shared_ptr<Pipe>> serverPipePromise;
        std::promise<Descriptor> readDescriptorPromise;
        std::promise<void> readMessagePromise;

        auto context = makeContext();

        auto listener = context->listen(genUrls());
        pg.send(PeerGroup::kClient, listener->url("uv"));

        listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
          if (error) {
            serverPipePromise.set_exception(
                std::make_exception_ptr(std::runtime_error(error.what())));
          } else {
            serverPipePromise.set_value(std::move(pipe));
          }
        });
        std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

        serverPipe->readDescriptor(
            [&readDescriptorPromise](
                const Error& error, Descriptor descriptor) {
              if (error) {
                readDescriptorPromise.set_exception(
                    std::make_exception_ptr(std::runtime_error(error.what())));
              } else {
                readDescriptorPromise.set_value(std::move(descriptor));
              }
            });

        Descriptor descriptor = readDescriptorPromise.get_future().get();
        Allocation allocation = allocateForDescriptor(descriptor, buffers);
        serverPipe->read(allocation, [&readMessagePromise](const Error& error) {
          if (error) {
            readMessagePromise.set_exception(
                std::make_exception_ptr(std::runtime_error(error.what())));
          } else {
            readMessagePromise.set_value();
          }
        });
        readMessagePromise.get_future().get();
        EXPECT_TRUE(descriptorAndAllocationMatchMessage(
            descriptor, allocation, makeMessage(kNumPayloads, kNumTensors)));

        pg.done(PeerGroup::kServer);
        pg.join(PeerGroup::kServer);

        context->join();
      },
      [&]() {
        std::promise<void> writtenMessagePromise;

        auto context = makeContext();

        auto url = pg.recv(PeerGroup::kClient);
        auto clientPipe = context->connect(url);

        clientPipe->write(
            makeMessage(kNumPayloads, kNumTensors),
            [&writtenMessagePromise](const Error& error) {
              if (error) {
                writtenMessagePromise.set_exception(
                    std::make_exception_ptr(std::runtime_error(error.what())));
              } else {
                writtenMessagePromise.set_value();
              }
            });
        writtenMessagePromise.get_future().get();

        pg.done(PeerGroup::kClient);
        pg.join(PeerGroup::kClient);

        context->join();
      });
}

TEST(Context, ClientPingInline) {
  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        std::vector<std::shared_ptr<void>> buffers;
        std::promise<std::shared_ptr<Pipe>> serverPipePromise;
        std::promise<void> readCompletedProm;

        auto context = makeContext();

        auto listener = context->listen(genUrls());
        pg.send(PeerGroup::kClient, listener->url("uv"));

        listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
          if (error) {
            serverPipePromise.set_exception(
                std::make_exception_ptr(std::runtime_error(error.what())));
          } else {
            serverPipePromise.set_value(std::move(pipe));
          }
        });
        std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

        serverPipe->readDescriptor([&serverPipe, &readCompletedProm, &buffers](
                                       const Error& error,
                                       Descriptor descriptor) {
          if (error) {
            ADD_FAILURE() << error.what();
            readCompletedProm.set_value();
            return;
          }

          Allocation allocation = allocateForDescriptor(descriptor, buffers);
          serverPipe->read(
              allocation,
              [&readCompletedProm,
               descriptor{std::move(descriptor)},
               allocation](const Error& error) {
                if (error) {
                  readCompletedProm.set_exception(std::make_exception_ptr(
                      std::runtime_error(error.what())));
                } else {
                  EXPECT_TRUE(descriptorAndAllocationMatchMessage(
                      descriptor,
                      allocation,
                      makeMessage(kNumPayloads, kNumTensors)));
                  readCompletedProm.set_value();
                }
              });
        });
        readCompletedProm.get_future().get();

        pg.done(PeerGroup::kServer);
        pg.join(PeerGroup::kServer);

        context->join();
      },
      [&]() {
        std::promise<void> writeCompletedProm;

        auto context = makeContext();

        auto url = pg.recv(PeerGroup::kClient);
        auto clientPipe = context->connect(url);

        clientPipe->write(
            makeMessage(kNumPayloads, kNumTensors),
            [&writeCompletedProm](const Error& error) {
              if (error) {
                writeCompletedProm.set_exception(
                    std::make_exception_ptr(std::runtime_error(error.what())));
              } else {
                writeCompletedProm.set_value();
              }
            });
        writeCompletedProm.get_future().get();

        pg.done(PeerGroup::kClient);
        pg.join(PeerGroup::kClient);

        context->join();
      });
}

TEST(Context, ServerPingPongTwice) {
  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        std::vector<std::shared_ptr<void>> buffers;
        std::promise<std::shared_ptr<Pipe>> serverPipePromise;
        std::promise<void> pingCompletedProm;

        auto context = makeContext();

        auto listener = context->listen(genUrls());
        pg.send(PeerGroup::kClient, listener->url("uv"));

        listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
          if (error) {
            serverPipePromise.set_exception(
                std::make_exception_ptr(std::runtime_error(error.what())));
          } else {
            serverPipePromise.set_value(std::move(pipe));
          }
        });
        std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

        int numPingsGoneThrough = 0;
        for (int i = 0; i < 2; i++) {
          serverPipe->write(
              makeMessage(kNumPayloads, kNumTensors),
              [&serverPipe,
               &pingCompletedProm,
               &buffers,
               &numPingsGoneThrough,
               i](const Error& error) {
                if (error) {
                  ADD_FAILURE() << error.what();
                  pingCompletedProm.set_value();
                  return;
                }
                serverPipe->readDescriptor(
                    [&serverPipe,
                     &pingCompletedProm,
                     &buffers,
                     &numPingsGoneThrough,
                     i](const Error& error, Descriptor descriptor) {
                      if (error) {
                        ADD_FAILURE() << error.what();
                        pingCompletedProm.set_value();
                        return;
                      }
                      Allocation allocation =
                          allocateForDescriptor(descriptor, buffers);
                      serverPipe->read(
                          allocation,
                          [&pingCompletedProm,
                           &numPingsGoneThrough,
                           descriptor{std::move(descriptor)},
                           allocation,
                           i](const Error& error) {
                            if (error) {
                              ADD_FAILURE() << error.what();
                              pingCompletedProm.set_value();
                              return;
                            }
                            EXPECT_TRUE(descriptorAndAllocationMatchMessage(
                                descriptor,
                                allocation,
                                makeMessage(kNumPayloads, kNumTensors)));
                            EXPECT_EQ(numPingsGoneThrough, i);
                            numPingsGoneThrough++;
                            if (numPingsGoneThrough == 2) {
                              pingCompletedProm.set_value();
                            }
                          });
                    });
              });
        }
        pingCompletedProm.get_future().get();

        pg.done(PeerGroup::kServer);
        pg.join(PeerGroup::kServer);

        context->join();
      },
      [&]() {
        std::vector<std::shared_ptr<void>> buffers;
        std::promise<void> pongCompletedProm;

        auto context = makeContext();

        auto url = pg.recv(PeerGroup::kClient);
        auto clientPipe = context->connect(url);

        int numPongsGoneThrough = 0;
        for (int i = 0; i < 2; i++) {
          clientPipe->readDescriptor([&clientPipe,
                                      &pongCompletedProm,
                                      &buffers,
                                      &numPongsGoneThrough,
                                      i](const Error& error,
                                         Descriptor descriptor) {
            if (error) {
              ADD_FAILURE() << error.what();
              pongCompletedProm.set_value();
              return;
            }
            Allocation allocation = allocateForDescriptor(descriptor, buffers);
            clientPipe->read(
                allocation,
                [&clientPipe,
                 &pongCompletedProm,
                 &numPongsGoneThrough,
                 descriptor{std::move(descriptor)},
                 allocation,
                 i](const Error& error) {
                  if (error) {
                    ADD_FAILURE() << error.what();
                    pongCompletedProm.set_value();
                    return;
                  }

                  // Copy received message to send it back.
                  Message message =
                      messageFromAllocation(descriptor, allocation);
                  clientPipe->write(
                      std::move(message),
                      [&pongCompletedProm, &numPongsGoneThrough, i](
                          const Error& error) {
                        if (error) {
                          ADD_FAILURE() << error.what();
                          pongCompletedProm.set_value();
                          return;
                        }
                        EXPECT_EQ(numPongsGoneThrough, i);
                        numPongsGoneThrough++;
                        if (numPongsGoneThrough == 2) {
                          pongCompletedProm.set_value();
                        }
                      });
                });
          });
        }
        pongCompletedProm.get_future().get();

        pg.done(PeerGroup::kClient);
        pg.join(PeerGroup::kClient);

        context->join();
      });
}

static void pipeRead(
    std::shared_ptr<Pipe>& pipe,
    std::vector<std::shared_ptr<void>>& buffers,
    std::function<void(const Error&, Descriptor, Allocation)> fn) {
  pipe->readDescriptor([&pipe, &buffers, fn{std::move(fn)}](
                           const Error& error, Descriptor descriptor) mutable {
    ASSERT_FALSE(error);
    Allocation allocation = allocateForDescriptor(descriptor, buffers);
    pipe->read(
        allocation,
        [fn{std::move(fn)}, descriptor{std::move(descriptor)}, allocation](
            const Error& error) mutable {
          fn(error, std::move(descriptor), std::move(allocation));
        });
  });
}

TEST(Context, MixedTensorMessage) {
  constexpr int kNumMessages = 2;

  ForkedThreadPeerGroup pg;
  pg.spawn(
      [&]() {
        std::vector<std::shared_ptr<void>> buffers;
        std::promise<std::shared_ptr<Pipe>> serverPipePromise;
        std::promise<void> readCompletedProm;

        auto context = makeContext();

        auto listener = context->listen(genUrls());
        pg.send(PeerGroup::kClient, listener->url("uv"));

        listener->accept([&](const Error& error, std::shared_ptr<Pipe> pipe) {
          if (error) {
            serverPipePromise.set_exception(
                std::make_exception_ptr(std::runtime_error(error.what())));
          } else {
            serverPipePromise.set_value(std::move(pipe));
          }
        });
        std::shared_ptr<Pipe> serverPipe = serverPipePromise.get_future().get();

        std::atomic<int> readNum(kNumMessages);
        pipeRead(
            serverPipe,
            buffers,
            [&readNum, &readCompletedProm](
                const Error& error,
                Descriptor descriptor,
                Allocation allocation) {
              ASSERT_FALSE(error);
              EXPECT_TRUE(descriptorAndAllocationMatchMessage(
                  descriptor,
                  allocation,
                  makeMessage(kNumPayloads, kNumTensors)));
              if (--readNum == 0) {
                readCompletedProm.set_value();
              }
            });
        pipeRead(
            serverPipe,
            buffers,
            [&readNum, &readCompletedProm](
                const Error& error,
                Descriptor descriptor,
                Allocation allocation) {
              ASSERT_FALSE(error);
              EXPECT_TRUE(descriptorAndAllocationMatchMessage(
                  descriptor, allocation, makeMessage(0, 0)));
              if (--readNum == 0) {
                readCompletedProm.set_value();
              }
            });
        readCompletedProm.get_future().get();

        pg.done(PeerGroup::kServer);
        pg.join(PeerGroup::kServer);

        context->join();
      },
      [&]() {
        std::promise<void> writeCompletedProm;

        auto context = makeContext();

        auto url = pg.recv(PeerGroup::kClient);
        auto clientPipe = context->connect(url);

        std::atomic<int> writeNum(kNumMessages);
        clientPipe->write(
            makeMessage(kNumPayloads, kNumTensors),
            [&writeNum, &writeCompletedProm](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              if (--writeNum == 0) {
                writeCompletedProm.set_value();
              }
            });
        clientPipe->write(
            makeMessage(0, 0),
            [&writeNum, &writeCompletedProm](const Error& error) {
              ASSERT_FALSE(error) << error.what();
              if (--writeNum == 0) {
                writeCompletedProm.set_value();
              }
            });
        writeCompletedProm.get_future().get();

        pg.done(PeerGroup::kClient);
        pg.join(PeerGroup::kClient);

        context->join();
      });
}
