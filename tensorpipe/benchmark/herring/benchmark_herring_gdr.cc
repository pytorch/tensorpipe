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

#include <ATen/cuda/CUDAEvent.h>
#include <c10/core/thread_pool.h>
#include <c10/cuda/CUDAStream.h>
#include <c10d/Store.hpp>
#include <nccl.h>
#include <pybind11/pybind11.h>
#include <tensorpipe/tensorpipe.h>
#include <tensorpipe/tensorpipe_cuda.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/torch.h>

#include "cuda_kernels.cuh"

namespace {

int64_t deltaAsUs(
    std::chrono::steady_clock::time_point start,
    std::chrono::steady_clock::time_point stop) {
  return std::chrono::duration_cast<std::chrono::microseconds>(stop - start)
      .count();
}

template <typename T>
T ceilOfRatio(T num, T den) {
  return (num - 1) / den + 1;
}

class CallbackBarrier {
 public:
  CallbackBarrier() = default;

  template <typename T>
  auto wrapCallback(T fn) {
    return wrapTask(
        [this, fn{std::move(fn)}](
            const tensorpipe::Error& error, auto&&... args) mutable {
          if (error) {
            LOG(ERROR) << error.what();
            std::unique_lock<std::mutex> lock(mutex_);
            if (!anyError_) {
              anyError_ = error;
            }
          } else {
            fn(std::forward<decltype(args)>(args)...);
          }
        });
  }

  template <typename T>
  auto wrapTask(T fn) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      numPendingCallbacks_ += 1;
    }
    return [this, fn{std::move(fn)}](auto&&... args) mutable {
      fn(std::forward<decltype(args)>(args)...);
      std::unique_lock<std::mutex> lock(mutex_);
      numPendingCallbacks_ -= 1;
      cv_.notify_all();
    };
  }

  void notifyExternalEventHappened() {
    std::unique_lock<std::mutex> lock(mutex_);
    numExternalEvents_ += 1;
    cv_.notify_all();
  }

  void waitForNextExternalEvent() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() {
      return numPendingCallbacks_ == 0 || numExternalEvents_ > 0;
    });
    if (anyError_) {
      throw std::runtime_error(anyError_.what());
    }
    if (numExternalEvents_ == 0) {
      throw std::runtime_error(
          "All callbacks terminated before an external event occurred");
    }
    numExternalEvents_ -= 1;
  }

  void join() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() {
      return numPendingCallbacks_ == 0 || numExternalEvents_ > 0;
    });
    if (anyError_) {
      throw std::runtime_error(anyError_.what());
    }
    if (numExternalEvents_ > 0) {
      throw std::runtime_error(
          "An external event occurred while waiting for callbacks to terminate");
    }
  }

  ~CallbackBarrier() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return numPendingCallbacks_ == 0; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  tensorpipe::Error anyError_ = tensorpipe::Error::kSuccess;
  size_t numPendingCallbacks_ = 0;
  size_t numExternalEvents_ = 0;
};

#define CUDA_CHECK(op)                        \
  {                                           \
    cudaError_t res = (op);                   \
    if (res != cudaSuccess) {                 \
      throw std::runtime_error("CUDA error"); \
    }                                         \
  }

#define NCCL_CHECK(op)                        \
  {                                           \
    ncclResult_t res = (op);                  \
    if (res != ncclSuccess) {                 \
      throw std::runtime_error("NCCL error"); \
    }                                         \
  }

struct NcclCommDeleter {
  void operator()(ncclComm_t comm) {
    NCCL_CHECK(ncclCommDestroy(comm));
  }
};

using NcclComm =
    std::unique_ptr<std::remove_pointer_t<ncclComm_t>, NcclCommDeleter>;

NcclComm createNcclComm(int rank, int worldSize, ncclUniqueId uniqueId) {
  ncclComm_t comm;
  NCCL_CHECK(ncclCommInitRank(&comm, worldSize, uniqueId, rank));
  return NcclComm(comm, NcclCommDeleter{});
}

std::shared_ptr<tensorpipe::Context> createTensorPipeContext(std::string name) {
  auto ctx = std::make_shared<tensorpipe::Context>(
      tensorpipe::ContextOptions().name(std::move(name)));
  ctx->registerTransport(0, "ibv", tensorpipe::transport::ibv::create());
  ctx->registerChannel(0, "cuda_gdr", tensorpipe::channel::cuda_gdr::create());
  return ctx;
}

// We need this extra named namespace inside our unnamed namespace because of
// https://github.com/pybind/pybind11/issues/3289
namespace benchmark_herring_gdr {

struct ServerStats {
  struct EpochStats {
    struct BucketStats {
      struct MachineStats {
        int64_t additionTime = 0;
        int64_t recvToSendTime = 0;
      };

      std::vector<MachineStats> machines;

      explicit BucketStats(size_t numMachines) : machines(numMachines) {}
    };

    std::vector<BucketStats> buckets;

    explicit EpochStats(size_t numBuckets, size_t numMachines)
        : buckets(numBuckets, BucketStats(numMachines)) {}
  };

  std::vector<EpochStats> epochs;

  explicit ServerStats(size_t numEpochs, size_t numBuckets, size_t numMachines)
      : epochs(numEpochs, EpochStats(numBuckets, numMachines)) {}
};

class Server {
 public:
  Server(
      size_t machineIdx,
      size_t deviceIdx,
      size_t numMachines,
      size_t numDevicesPerMachine,
      size_t numBuckets,
      size_t bucketSize,
      size_t numEpochs,
      c10::intrusive_ptr<c10d::Store> store)
      : machineIdx_(machineIdx),
        deviceIdx_(deviceIdx),
        numMachines_(numMachines),
        numBuckets_(numBuckets),
        sliceLen_(
            (bucketSize / numDevicesPerMachine) * (machineIdx_ + 1) /
                numMachines_ -
            (bucketSize / numDevicesPerMachine) * machineIdx_ / numMachines_),
        numEpochs_(numEpochs),
        store_(std::move(store)),
        context_(createTensorPipeContext("s" + std::to_string(machineIdx_))),
        stats_(numBuckets, numMachines),
        recvTimes_(
            numBuckets,
            std::vector<std::chrono::steady_clock::time_point>(
                numMachines,
                std::chrono::steady_clock::time_point())) {}

  ServerStats run() {
    allocateTensors();
    startListening();
    waitForIncomingPipes();
    ServerStats stats(numEpochs_, numBuckets_, numMachines_);
    for (size_t epochIdx = 0; epochIdx < numEpochs_; epochIdx += 1) {
      setTensorsToZero();
      runOneEpoch();
      stats.epochs[epochIdx] = stats_;
    }

    // @nocommit
    // Ugly hack to prevent the server's TP context from shutting down before
    // the clients have received all the data.
    std::this_thread::sleep_for(std::chrono::seconds(5));

    return stats;
  }

 private:
  const size_t machineIdx_;
  const size_t deviceIdx_;
  const size_t numMachines_;
  const size_t numBuckets_;
  const size_t sliceLen_;
  const size_t numEpochs_;
  const c10::intrusive_ptr<c10d::Store> store_;
  const std::shared_ptr<tensorpipe::Context> context_;
  std::shared_ptr<tensorpipe::Listener> listener_;
  std::vector<std::shared_ptr<tensorpipe::Pipe>> pipes_;
  std::vector<torch::Tensor> buckets_;
  std::vector<std::vector<torch::Tensor>> stagingTensors_;
  ServerStats::EpochStats stats_;
  std::vector<std::vector<std::chrono::steady_clock::time_point>> recvTimes_;

  void allocateTensors() {
    buckets_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_.push_back(torch::empty(
          sliceLen_,
          c10::TensorOptions()
              .dtype(c10::kFloat)
              .device(c10::Device(c10::kCUDA, 0))));
    }

    stagingTensors_.resize(numMachines_);
    for (size_t machineIdx = 0; machineIdx < numMachines_; machineIdx += 1) {
      stagingTensors_[machineIdx].reserve(numBuckets_);
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        stagingTensors_[machineIdx].push_back(torch::empty(
            sliceLen_,
            c10::TensorOptions()
                .dtype(c10::kFloat)
                .device(c10::Device(c10::kCUDA, 0))));
      }
    }
  }

  void startListening() {
    tensorpipe::Error error;
    std::string address;
    const char* iface = std::getenv("TP_SOCKET_IFNAME");
    std::tie(error, address) = iface != nullptr
        ? tensorpipe::transport::ibv::lookupAddrForIface(std::string(iface))
        : tensorpipe::transport::ibv::lookupAddrForHostname();
    if (error) {
      throw std::runtime_error(error.what());
    }
    listener_ = context_->listen({
        "ibv://" + std::move(address),
    });

    std::string key = "machines/" + std::to_string(machineIdx_) + "/servers/" +
        std::to_string(deviceIdx_) + "/address";
    std::string concreteAddress = listener_->url("ibv");
    store_->set(
        key,
        std::vector<uint8_t>(concreteAddress.begin(), concreteAddress.end()));
  }

  void waitForIncomingPipes() {
    CallbackBarrier barrier;

    pipes_.resize(numMachines_);
    for (size_t clientMachineIdx = 0; clientMachineIdx < numMachines_;
         clientMachineIdx += 1) {
      listener_->accept(barrier.wrapCallback(
          [&, this](std::shared_ptr<tensorpipe::Pipe> pipe) {
            int otherClientMachineIdx = std::strtol(
                pipe->getRemoteName().c_str() + 1, nullptr, /*base=*/10);
            pipes_[otherClientMachineIdx] = std::move(pipe);
          }));
    }

    barrier.join();
  }

  void setTensorsToZero() {
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_[bucketIdx].fill_(0);
    }
  }

  void runOneEpoch() {
    c10::cuda::CUDAStream recvStream =
        c10::cuda::getStreamFromPool(/*isHighPriority=*/true, /*device=*/0);
    std::vector<c10::cuda::CUDAStream> computeStreams;
    computeStreams.reserve(numMachines_);
    for (size_t otherMachineIdx = 0; otherMachineIdx < numMachines_;
         otherMachineIdx += 1) {
      computeStreams.push_back(
          c10::cuda::getStreamFromPool(/*isHighPriority=*/true, /*device=*/0));
    }
    c10::cuda::CUDAStream sendStream =
        c10::cuda::getStreamFromPool(/*isHighPriority=*/true, /*device=*/0);

    std::vector<std::vector<at::cuda::CUDAEvent>> events;
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      events.emplace_back(numMachines_);
    }

    CallbackBarrier barrier;

    std::mutex mutex;
    std::vector<size_t> numClientsDoneForBucket(numBuckets_, 0);
    std::vector<bool> hasBucketBeenSent(numBuckets_, false);

    for (size_t machineIdx = 0; machineIdx < numMachines_; machineIdx += 1) {
      tensorpipe::Pipe& pipe = *pipes_[machineIdx];
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        torch::Tensor& bucket = buckets_[bucketIdx];
        torch::Tensor& stagingTensor = stagingTensors_[machineIdx][bucketIdx];

        pipe.readDescriptor(barrier.wrapCallback(
            [&, machineIdx, bucketIdx](tensorpipe::Descriptor /* unused */) {
              tensorpipe::Allocation allocation;
              allocation.tensors.resize(1);
              allocation.tensors[0].buffer = tensorpipe::CudaBuffer{
                  .ptr = stagingTensor.data_ptr(),
                  .stream = recvStream.stream(),
              };
              pipe.read(
                  std::move(allocation),
                  barrier.wrapCallback([&, machineIdx, bucketIdx]() {
                    recvTimes_[bucketIdx][machineIdx] =
                        std::chrono::steady_clock::now();
                    {
                      at::cuda::CUDAEvent event;
                      event.record(recvStream);
                      event.block(computeStreams[machineIdx]);
                    }
                    {
                      std::chrono::steady_clock::time_point additionStartTime =
                          std::chrono::steady_clock::now();
                      atomicAddInto(
                          buckets_[bucketIdx].data_ptr<float>(),
                          stagingTensor.data_ptr<float>(),
                          bucket.numel(),
                          computeStreams[machineIdx].stream());
                      stats_.buckets[bucketIdx]
                          .machines[machineIdx]
                          .additionTime = deltaAsUs(
                          additionStartTime, std::chrono::steady_clock::now());
                    }
                    events[bucketIdx][machineIdx].record(
                        computeStreams[machineIdx]);
                    std::unique_lock<std::mutex> lock(mutex);
                    numClientsDoneForBucket[bucketIdx] += 1;
                    for (size_t otherBucketIdx = 0;
                         otherBucketIdx < numBuckets_;
                         otherBucketIdx += 1) {
                      if (hasBucketBeenSent[otherBucketIdx]) {
                        continue;
                      }
                      if (numClientsDoneForBucket[otherBucketIdx] <
                          numMachines_) {
                        break;
                      }
                      for (size_t otherMachineIdx = 0;
                           otherMachineIdx < numMachines_;
                           otherMachineIdx += 1) {
                        events[bucketIdx][otherMachineIdx].block(sendStream);
                      }
                      for (size_t otherMachineIdx = 0;
                           otherMachineIdx < numMachines_;
                           otherMachineIdx += 1) {
                        tensorpipe::Pipe& pipe = *pipes_[otherMachineIdx];
                        tensorpipe::Message message;
                        message.tensors.resize(1);
                        message.tensors[0] = {
                            .buffer =
                                tensorpipe::CudaBuffer{
                                    .ptr = bucket.data_ptr(),
                                    .stream = sendStream.stream(),
                                },
                            .length = bucket.nbytes(),
                            .targetDevice = tensorpipe::Device(
                                tensorpipe::kCudaDeviceType, 0),
                        };
                        stats_.buckets[bucketIdx]
                            .machines[otherMachineIdx]
                            .recvToSendTime = deltaAsUs(
                            recvTimes_[bucketIdx][otherMachineIdx],
                            std::chrono::steady_clock::now());
                        pipe.write(
                            std::move(message), barrier.wrapCallback([]() {}));
                      }
                      hasBucketBeenSent[otherBucketIdx] = true;
                    }
                  }));
            }));
      }
    }

    barrier.join();
  }
};

struct ClientStats {
  struct EpochStats {
    struct BucketStats {
      struct ServerStats {
        int64_t transferTime = 0;
      };

      int64_t ncclReduceScatterTime = 0;
      int64_t ncclAllGatherTime = 0;
      std::vector<ServerStats> servers;

      explicit BucketStats(size_t numServers) : servers(numServers) {}
    };

    int64_t endToEndTime = 0;
    std::vector<BucketStats> buckets;

    explicit EpochStats(size_t numBuckets, size_t numServers)
        : buckets(numBuckets, BucketStats(numServers)) {}
  };

  std::vector<EpochStats> epochs;

  explicit ClientStats(size_t numEpochs, size_t numBuckets, size_t numServers)
      : epochs(numEpochs, EpochStats(numBuckets, numServers)) {}
};

class Client {
 public:
  Client(
      size_t machineIdx,
      size_t deviceIdx,
      size_t numMachines,
      size_t numDevicesPerMachine,
      size_t numBuckets,
      size_t bucketSize,
      size_t numEpochs,
      c10::intrusive_ptr<c10d::Store> store)
      : machineIdx_(machineIdx),
        deviceIdx_(deviceIdx),
        numMachines_(numMachines),
        numDevicesPerMachine_(numDevicesPerMachine),
        numBuckets_(numBuckets),
        bucketSize_(bucketSize),
        numEpochs_(numEpochs),
        store_(std::move(store)),
        context_(createTensorPipeContext("c" + std::to_string(machineIdx))),
        stats_(numBuckets, numMachines),
        ncclAllGatherStartTimes_(
            numBuckets,
            std::chrono::steady_clock::time_point()) {}

  ClientStats run() {
    allocateTensors();
    setUpNccl();
    connectToServers();
    ClientStats stats(numEpochs_, numBuckets_, numMachines_);
    for (size_t epochIdx = 0; epochIdx < numEpochs_; epochIdx += 1) {
      setTensorsToOne();
      runOneEpoch();
      checkTensors();
      stats.epochs[epochIdx] = stats_;
    }
    return stats;
  }

 private:
  const size_t machineIdx_;
  const size_t deviceIdx_;
  const size_t numMachines_;
  const size_t numDevicesPerMachine_;
  const size_t numBuckets_;
  const size_t bucketSize_;
  const size_t numEpochs_;
  const c10::intrusive_ptr<c10d::Store> store_;
  const std::shared_ptr<tensorpipe::Context> context_;
  std::vector<std::shared_ptr<tensorpipe::Pipe>> pipes_;
  std::vector<torch::Tensor> buckets_;
  std::vector<torch::Tensor> stagingTensors_;
  NcclComm ncclComm_;
  ClientStats::EpochStats stats_;
  std::vector<std::chrono::steady_clock::time_point> ncclAllGatherStartTimes_;

  void allocateTensors() {
    buckets_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_.push_back(torch::empty(
          bucketSize_,
          c10::TensorOptions()
              .dtype(c10::kFloat)
              .device(c10::Device(c10::kCUDA, 0))));
    }

    assert(bucketSize_ % numDevicesPerMachine_ == 0);
    stagingTensors_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      stagingTensors_.push_back(torch::empty(
          bucketSize_ / numDevicesPerMachine_,
          c10::TensorOptions()
              .dtype(c10::kFloat)
              .device(c10::Device(c10::kCUDA, 0))));
    }
  }

  void setUpNccl() {
    ncclUniqueId uniqueId;
    if (deviceIdx_ == 0) {
      NCCL_CHECK(ncclGetUniqueId(&uniqueId));
      store_->set(
          "machines/" + std::to_string(machineIdx_) + "/nccl_id",
          std::vector<uint8_t>(
              reinterpret_cast<uint8_t*>(&uniqueId),
              reinterpret_cast<uint8_t*>(&uniqueId) + sizeof(ncclUniqueId)));
    } else {
      std::vector<uint8_t> uniqueIdData =
          store_->get("machines/" + std::to_string(machineIdx_) + "/nccl_id");
      std::memcpy(&uniqueId, uniqueIdData.data(), sizeof(ncclUniqueId));
    }
    ncclComm_ = createNcclComm(
        /*rank=*/deviceIdx_,
        /*worldSize=*/numDevicesPerMachine_,
        uniqueId);
  }

  void connectToServers() {
    pipes_.resize(numMachines_);
    for (size_t otherMachineIdx = 0; otherMachineIdx < numMachines_;
         otherMachineIdx += 1) {
      std::vector<uint8_t> addressData = store_->get(
          "machines/" + std::to_string(otherMachineIdx) + "/servers/" +
          std::to_string(deviceIdx_) + "/address");
      std::string address((char*)addressData.data(), addressData.size());
      pipes_[otherMachineIdx] = context_->connect(std::move(address));
    }
  }

  void setTensorsToOne() {
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_[bucketIdx].fill_(1);
    }
  }

  void runOneEpoch() {
    c10::cuda::CUDAStream stream =
        c10::cuda::getStreamFromPool(/*isHighPriority=*/true, /*device=*/0);

    std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();

    CallbackBarrier barrier;

    std::vector<at::cuda::CUDAEvent> reduceScatterEvents(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      NCCL_CHECK(ncclReduceScatter(
          buckets_[bucketIdx].data_ptr(),
          stagingTensors_[bucketIdx].data_ptr(),
          bucketSize_ / numDevicesPerMachine_,
          ncclFloat,
          ncclSum,
          ncclComm_.get(),
          stream));
      reduceScatterEvents[bucketIdx].record(stream);
    }

    std::mutex mutex;
    std::vector<size_t> numServersDoneForBucket(numBuckets_, 0);
    std::vector<at::cuda::CUDAEvent> allGatherEvents(numBuckets_);

    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      reduceScatterEvents[bucketIdx].synchronize();
      stats_.buckets[bucketIdx].ncclReduceScatterTime =
          deltaAsUs(start, std::chrono::steady_clock::now());

      torch::Tensor& stagingTensor = stagingTensors_[bucketIdx];
      for (size_t serverMachineIdx = 0; serverMachineIdx < numMachines_;
           serverMachineIdx += 1) {
        size_t startPos = (bucketSize_ / numDevicesPerMachine_) *
            serverMachineIdx / numMachines_;
        size_t endPos = (bucketSize_ / numDevicesPerMachine_) *
            (serverMachineIdx + 1) / numMachines_;
        torch::Tensor slice =
            stagingTensor.slice(/*dim=*/0, /*start=*/startPos, /*end=*/endPos);
        tensorpipe::Message message;
        message.tensors.resize(1);
        message.tensors[0] = {
            .buffer =
                tensorpipe::CudaBuffer{
                    .ptr = slice.data_ptr(),
                    .stream = stream.stream(),
                },
            .length = slice.nbytes(),
            .targetDevice = tensorpipe::Device(tensorpipe::kCudaDeviceType, 0),
        };
        tensorpipe::Pipe& pipe = *pipes_[serverMachineIdx];
        std::chrono::steady_clock::time_point transferStartTime =
            std::chrono::steady_clock::now();
        pipe.write(
            std::move(message),
            barrier.wrapCallback(
                [&, bucketIdx, serverMachineIdx, slice, transferStartTime]() {
                  pipe.readDescriptor(barrier.wrapCallback(
                      [&,
                       bucketIdx,
                       serverMachineIdx,
                       slice,
                       transferStartTime](tensorpipe::Descriptor /* unused */) {
                        tensorpipe::Allocation allocation;
                        allocation.tensors.resize(1);
                        allocation.tensors[0].buffer = tensorpipe::CudaBuffer{
                            .ptr = slice.data_ptr(),
                            .stream = stream.stream(),
                        };
                        pipe.read(
                            std::move(allocation),
                            barrier.wrapCallback([&,
                                                  bucketIdx,
                                                  serverMachineIdx,
                                                  transferStartTime]() {
                              stats_.buckets[bucketIdx]
                                  .servers[serverMachineIdx]
                                  .transferTime = deltaAsUs(
                                  transferStartTime,
                                  std::chrono::steady_clock::now());
                              std::unique_lock<std::mutex> lock(mutex);
                              numServersDoneForBucket[bucketIdx] += 1;
                              if (numServersDoneForBucket[bucketIdx] <
                                  numMachines_) {
                                return;
                              }
                              ncclAllGatherStartTimes_[bucketIdx] =
                                  std::chrono::steady_clock::now();
                              NCCL_CHECK(ncclAllGather(
                                  stagingTensors_[bucketIdx].data_ptr(),
                                  buckets_[bucketIdx].data_ptr(),
                                  bucketSize_ / numDevicesPerMachine_,
                                  ncclFloat,
                                  ncclComm_.get(),
                                  stream));
                              allGatherEvents[bucketIdx].record(stream);
                              barrier.notifyExternalEventHappened();
                            }));
                      }));
                }));
      }
    }

    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      barrier.waitForNextExternalEvent();
      allGatherEvents[bucketIdx].synchronize();
      stats_.buckets[bucketIdx].ncclAllGatherTime = deltaAsUs(
          ncclAllGatherStartTimes_[bucketIdx],
          std::chrono::steady_clock::now());
    }

    barrier.join();
    stream.synchronize();

    stats_.endToEndTime = deltaAsUs(start, std::chrono::steady_clock::now());
  }

  void checkTensors() {
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      if (!buckets_[bucketIdx].allclose(torch::full(
              {},
              static_cast<float>(numMachines_ * numDevicesPerMachine_),
              c10::TensorOptions()
                  .dtype(c10::kFloat)
                  .device(c10::Device(c10::kCUDA, 0))))) {
        throw std::runtime_error("Bad result");
      }
    }
  }
};

} // namespace benchmark_herring_gdr
} // namespace

namespace py = pybind11;

template <typename T>
using shared_ptr_class_ = py::class_<T, std::shared_ptr<T>>;

PYBIND11_MODULE(benchmark_herring_gdr, module) {
  shared_ptr_class_<benchmark_herring_gdr::Server> server(module, "Server");
  shared_ptr_class_<benchmark_herring_gdr::Client> client(module, "Client");

  py::class_<
      benchmark_herring_gdr::ServerStats::EpochStats::BucketStats::MachineStats>
      serverStatsEpochBucketMachine(module, "ServerStatsEpochBucketMachine");
  serverStatsEpochBucketMachine.def_readonly(
      "addition_time",
      &benchmark_herring_gdr::ServerStats::EpochStats::BucketStats::
          MachineStats::additionTime);
  serverStatsEpochBucketMachine.def_readonly(
      "recv_to_send_time",
      &benchmark_herring_gdr::ServerStats::EpochStats::BucketStats::
          MachineStats::recvToSendTime);

  py::class_<benchmark_herring_gdr::ServerStats::EpochStats::BucketStats>
      serverStatsEpochBucket(module, "ServerStatsEpochBucket");
  serverStatsEpochBucket.def_readonly(
      "machines",
      &benchmark_herring_gdr::ServerStats::EpochStats::BucketStats::machines);

  py::class_<benchmark_herring_gdr::ServerStats::EpochStats> serverStatsEpoch(
      module, "ServerStatsEpoch");
  serverStatsEpoch.def_readonly(
      "buckets", &benchmark_herring_gdr::ServerStats::EpochStats::buckets);

  py::class_<benchmark_herring_gdr::ServerStats> serverStats(
      module, "ServerStats");
  serverStats.def_readonly(
      "epochs", &benchmark_herring_gdr::ServerStats::epochs);

  server.def(
      py::init<
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          const c10::intrusive_ptr<c10d::Store>&>(),
      py::arg("machine_idx"),
      py::arg("device_idx"),
      py::arg("num_machines"),
      py::arg("num_devices_per_machine"),
      py::arg("num_buckets"),
      py::arg("bucket_size"),
      py::arg("num_epochs"),
      py::arg("store"));
  server.def(
      "run",
      &benchmark_herring_gdr::Server::run,
      py::call_guard<py::gil_scoped_release>());

  py::class_<
      benchmark_herring_gdr::ClientStats::EpochStats::BucketStats::ServerStats>
      clientStatsEpochBucketMachine(module, "ClientStatsEpochBucketMachine");
  clientStatsEpochBucketMachine.def_readonly(
      "transfer_time",
      &benchmark_herring_gdr::ClientStats::EpochStats::BucketStats::
          ServerStats::transferTime);

  py::class_<benchmark_herring_gdr::ClientStats::EpochStats::BucketStats>
      clientStatsEpochBucket(module, "ClientStatsEpochBucket");
  clientStatsEpochBucket.def_readonly(
      "servers",
      &benchmark_herring_gdr::ClientStats::EpochStats::BucketStats::servers);
  clientStatsEpochBucket.def_readonly(
      "nccl_all_gather_time",
      &benchmark_herring_gdr::ClientStats::EpochStats::BucketStats::
          ncclAllGatherTime);
  clientStatsEpochBucket.def_readonly(
      "nccl_reduce_scatter_time",
      &benchmark_herring_gdr::ClientStats::EpochStats::BucketStats::
          ncclReduceScatterTime);

  py::class_<benchmark_herring_gdr::ClientStats::EpochStats> clientStatsEpoch(
      module, "ClientStatsEpoch");
  clientStatsEpoch.def_readonly(
      "buckets", &benchmark_herring_gdr::ClientStats::EpochStats::buckets);
  clientStatsEpoch.def_readonly(
      "end_to_end_time",
      &benchmark_herring_gdr::ClientStats::EpochStats::endToEndTime);

  py::class_<benchmark_herring_gdr::ClientStats> clientStats(
      module, "ClientStats");
  clientStats.def_readonly(
      "epochs", &benchmark_herring_gdr::ClientStats::epochs);

  client.def(
      py::init<
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          const c10::intrusive_ptr<c10d::Store>&>(),
      py::arg("machine_idx"),
      py::arg("device_idx"),
      py::arg("num_machines"),
      py::arg("num_devices_per_machine"),
      py::arg("num_buckets"),
      py::arg("bucket_size"),
      py::arg("num_epochs"),
      py::arg("store"));
  client.def(
      "run",
      &benchmark_herring_gdr::Client::run,
      py::call_guard<py::gil_scoped_release>());
}
