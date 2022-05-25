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
#include <torch/csrc/utils/pybind.h>
#include <torch/torch.h>

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
  ctx->registerTransport(0, "uv", tensorpipe::transport::uv::create());
  ctx->registerChannel(0, "basic", tensorpipe::channel::basic::create());
  return ctx;
}

constexpr size_t kParamsPerLock = 1024;

// We need this extra named namespace inside our unnamed namespace because of
// https://github.com/pybind/pybind11/issues/3289
namespace benchmark_herring_tcp {

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
      size_t numMachines,
      size_t numDevicesPerMachine,
      size_t numBuckets,
      size_t bucketSize,
      size_t numEpochs,
      c10::intrusive_ptr<c10d::Store> store,
      size_t numThreads)
      : machineIdx_(machineIdx),
        numMachines_(numMachines),
        numBuckets_(numBuckets),
        bucketSize_(bucketSize),
        sliceLen_(
            (machineIdx_ + 1) * bucketSize_ / numMachines_ -
            machineIdx_ * bucketSize_ / numMachines_),
        numEpochs_(numEpochs),
        store_(std::move(store)),
        contexts_([&]() {
          std::vector<std::shared_ptr<tensorpipe::Context>> res(numMachines);
          for (size_t machineIdx = 0; machineIdx < numMachines;
               machineIdx += 1) {
            res[machineIdx] =
                createTensorPipeContext(std::to_string(machineIdx));
          }
          return res;
        }()),
        threadPool_(numThreads),
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
    return stats;
  }

 private:
  const size_t machineIdx_;
  const size_t numMachines_;
  const size_t numBuckets_;
  const size_t bucketSize_;
  const size_t sliceLen_;
  const size_t numEpochs_;
  const c10::intrusive_ptr<c10d::Store> store_;
  const std::vector<std::shared_ptr<tensorpipe::Context>> contexts_;
  std::vector<std::shared_ptr<tensorpipe::Listener>> listeners_;
  std::vector<std::shared_ptr<tensorpipe::Pipe>> pipes_;
  std::vector<torch::Tensor> buckets_;
  std::vector<std::vector<std::atomic_flag>> bucketLocks_;
  std::vector<std::vector<torch::Tensor>> stagingTensors_;
  c10::ThreadPool threadPool_;
  ServerStats::EpochStats stats_;
  std::vector<std::vector<std::chrono::steady_clock::time_point>> recvTimes_;

  void allocateTensors() {
    buckets_.reserve(numBuckets_);
    bucketLocks_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_.push_back(torch::empty(sliceLen_, c10::kFloat));
      size_t numChunks = ceilOfRatio(sliceLen_, kParamsPerLock);
      bucketLocks_.push_back(std::vector<std::atomic_flag>(numChunks));
      for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
        bucketLocks_[bucketIdx][chunkIdx].clear();
      }
    }

    stagingTensors_.resize(numMachines_);
    for (size_t machineIdx = 0; machineIdx < numMachines_; machineIdx += 1) {
      stagingTensors_[machineIdx].reserve(numBuckets_);
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        stagingTensors_[machineIdx].push_back(
            torch::empty(sliceLen_, c10::kFloat));
      }
    }
  }

  void startListening() {
    listeners_.resize(numMachines_);
    for (size_t machineIdx = 0; machineIdx < numMachines_; machineIdx += 1) {
      tensorpipe::Error error;
      std::string address;
      const char* iface = std::getenv("TP_SOCKET_IFNAME");
      std::tie(error, address) = iface != nullptr
          ? tensorpipe::transport::uv::lookupAddrForIface(std::string(iface))
          : tensorpipe::transport::uv::lookupAddrForHostname();
      if (error) {
        throw std::runtime_error(error.what());
      }
      listeners_[machineIdx] = contexts_[machineIdx]->listen({
          "uv://" + std::move(address),
      });

      std::string key = "servers/" + std::to_string(machineIdx_) + "/clients/" +
          std::to_string(machineIdx) + "/address";
      std::string concreteAddress = listeners_[machineIdx]->url("uv");
      store_->set(
          key,
          std::vector<uint8_t>(concreteAddress.begin(), concreteAddress.end()));
    }
  }

  void waitForIncomingPipes() {
    CallbackBarrier barrier;

    pipes_.resize(numMachines_);
    for (size_t clientMachineIdx = 0; clientMachineIdx < numMachines_;
         clientMachineIdx += 1) {
      listeners_[clientMachineIdx]->accept(barrier.wrapCallback(
          [&, this](std::shared_ptr<tensorpipe::Pipe> pipe) {
            int otherClientMachineIdx = std::strtol(
                pipe->getRemoteName().c_str(), nullptr, /*base=*/10);
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

  void addToBucket(size_t bucketIdx, torch::Tensor increment) {
    torch::Tensor& bucket = buckets_[bucketIdx];
    std::vector<std::atomic_flag>& locks = bucketLocks_[bucketIdx];
    size_t numChunks = ceilOfRatio(sliceLen_, kParamsPerLock);
    for (size_t chunkIdx = 0; chunkIdx < numChunks; chunkIdx += 1) {
      size_t chunkStart = chunkIdx * kParamsPerLock;
      size_t chunkEnd = std::min(sliceLen_, (chunkIdx + 1) * kParamsPerLock);
      bool wasLocked;
      do {
        wasLocked = locks[chunkIdx].test_and_set(std::memory_order_acquire);
      } while (wasLocked);
      bucket.slice(/*dim=*/0, /*start=*/chunkStart, /*end=*/chunkEnd) +=
          increment.slice(/*dim=*/0, /*start=*/chunkStart, /*end=*/chunkEnd);
      locks[chunkIdx].clear(std::memory_order_release);
    }
  }

  void runOneEpoch() {
    CallbackBarrier barrier;

    std::mutex mutex;
    std::vector<size_t> numClientsDoneForBucket(numBuckets_, 0);
    std::vector<bool> hasBucketBeenSent(numBuckets_, false);

    for (size_t machineIdx = 0; machineIdx < numMachines_; machineIdx += 1) {
      tensorpipe::Pipe& pipe = *pipes_[machineIdx];
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        torch::Tensor& bucket = buckets_[bucketIdx];
        torch::Tensor& stagingTensor = stagingTensors_[machineIdx][bucketIdx];

        pipe.readDescriptor(barrier.wrapCallback([&, machineIdx, bucketIdx](
                                                     tensorpipe::Descriptor
                                                     /* unused */) {
          tensorpipe::Allocation allocation;
          allocation.tensors.resize(1);
          allocation.tensors[0].buffer = tensorpipe::CpuBuffer{
              .ptr = stagingTensor.data_ptr(),
          };
          pipe.read(
              std::move(allocation),
              barrier.wrapCallback([&, machineIdx, bucketIdx]() {
                recvTimes_[bucketIdx][machineIdx] =
                    std::chrono::steady_clock::now();
                threadPool_.run(barrier.wrapTask([&, machineIdx, bucketIdx]() {
                  {
                    std::chrono::steady_clock::time_point additionStartTime =
                        std::chrono::steady_clock::now();
                    addToBucket(bucketIdx, stagingTensor);
                    stats_.buckets[bucketIdx]
                        .machines[machineIdx]
                        .additionTime = deltaAsUs(
                        additionStartTime, std::chrono::steady_clock::now());
                  }
                  std::unique_lock<std::mutex> lock(mutex);
                  numClientsDoneForBucket[bucketIdx] += 1;
                  for (size_t otherBucketIdx = 0; otherBucketIdx < numBuckets_;
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
                      tensorpipe::Pipe& pipe = *pipes_[otherMachineIdx];
                      tensorpipe::Message message;
                      message.tensors.resize(1);
                      message.tensors[0] = {
                          .buffer =
                              tensorpipe::CpuBuffer{
                                  .ptr = bucket.data_ptr(),
                              },
                          .length = bucket.nbytes(),
                          .targetDevice =
                              tensorpipe::Device(tensorpipe::kCpuDeviceType, 0),
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

      int64_t ncclReduceTime = 0;
      int64_t ncclBroadcastTime = 0;
      std::vector<ServerStats> servers;

      explicit BucketStats(size_t numMachines) : servers(numMachines) {}
    };

    int64_t endToEndTime = 0;
    std::vector<BucketStats> buckets;

    explicit EpochStats(size_t numBuckets, size_t numMachines)
        : buckets(numBuckets, BucketStats(numMachines)) {}
  };

  std::vector<EpochStats> epochs;

  explicit ClientStats(size_t numEpochs, size_t numBuckets, size_t numMachines)
      : epochs(numEpochs, EpochStats(numBuckets, numMachines)) {}
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
        contexts_([&]() {
          std::vector<std::shared_ptr<tensorpipe::Context>> res(numMachines);
          if (deviceIdx_ == 0) {
            for (size_t serverMachineIdx = 0; serverMachineIdx < numMachines;
                 serverMachineIdx += 1) {
              res[serverMachineIdx] =
                  createTensorPipeContext(std::to_string(machineIdx));
            }
          }
          return res;
        }()),
        stats_(numBuckets, numMachines),
        ncclBroadcastStartTimes_(
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
  const std::vector<std::shared_ptr<tensorpipe::Context>> contexts_;
  std::vector<std::shared_ptr<tensorpipe::Pipe>> pipes_;
  std::vector<torch::Tensor> buckets_;
  tensorpipe::optional<std::vector<torch::Tensor>> stagingTensors_;
  NcclComm ncclComm_;
  ClientStats::EpochStats stats_;
  std::vector<std::chrono::steady_clock::time_point> ncclBroadcastStartTimes_;

  void allocateTensors() {
    buckets_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_.push_back(torch::empty(
          bucketSize_,
          c10::TensorOptions()
              .dtype(c10::kFloat)
              .device(c10::Device(c10::kCUDA, 0))));
    }

    if (deviceIdx_ == 0) {
      stagingTensors_.emplace();
      stagingTensors_->reserve(numBuckets_);
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        stagingTensors_->push_back(torch::empty(
            bucketSize_,
            c10::TensorOptions().dtype(c10::kFloat).pinned_memory(true)));
      }
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
    if (deviceIdx_ == 0) {
      pipes_.resize(numMachines_);
      for (size_t serverMachineIdx = 0; serverMachineIdx < numMachines_;
           serverMachineIdx += 1) {
        std::vector<uint8_t> addressData = store_->get(
            "servers/" + std::to_string(serverMachineIdx) + "/clients/" +
            std::to_string(machineIdx_) + "/address");
        std::string address((char*)addressData.data(), addressData.size());
        pipes_[serverMachineIdx] =
            contexts_[serverMachineIdx]->connect(std::move(address));
      }
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

    if (deviceIdx_ == 0) {
      CallbackBarrier barrier;

      std::vector<at::cuda::CUDAEvent> reduceEvents(numBuckets_);
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        NCCL_CHECK(ncclReduce(
            buckets_[bucketIdx].data_ptr(),
            stagingTensors_.value()[bucketIdx].data_ptr(),
            bucketSize_,
            ncclFloat,
            ncclSum,
            0,
            ncclComm_.get(),
            stream));
        reduceEvents[bucketIdx].record(stream);
      }

      std::mutex mutex;
      std::condition_variable cv;
      std::vector<size_t> numMachinesDoneForBucket(numBuckets_, 0);
      std::vector<at::cuda::CUDAEvent> broadcastEvents(numBuckets_);

      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        reduceEvents[bucketIdx].synchronize();
        stats_.buckets[bucketIdx].ncclReduceTime =
            deltaAsUs(start, std::chrono::steady_clock::now());

        torch::Tensor& stagingTensor = stagingTensors_.value()[bucketIdx];
        for (size_t serverMachineIdx = 0; serverMachineIdx < numMachines_;
             serverMachineIdx += 1) {
          size_t startPos = bucketSize_ * serverMachineIdx / numMachines_;
          size_t endPos = bucketSize_ * (serverMachineIdx + 1) / numMachines_;
          torch::Tensor slice = stagingTensor.slice(
              /*dim=*/0, /*start=*/startPos, /*end=*/endPos);
          tensorpipe::Message message;
          message.tensors.resize(1);
          message.tensors[0] = {
              .buffer =
                  tensorpipe::CpuBuffer{
                      .ptr = slice.data_ptr(),
                  },
              .length = slice.nbytes(),
              .targetDevice = tensorpipe::Device(tensorpipe::kCpuDeviceType, 0),
          };
          tensorpipe::Pipe& pipe = *pipes_[serverMachineIdx];
          std::chrono::steady_clock::time_point transferStartTime =
              std::chrono::steady_clock::now();
          pipe.write(
              std::move(message),
              barrier.wrapCallback([&,
                                    bucketIdx,
                                    serverMachineIdx,
                                    slice,
                                    transferStartTime]() {
                pipe.readDescriptor(barrier.wrapCallback(
                    [&, bucketIdx, serverMachineIdx, slice, transferStartTime](
                        tensorpipe::Descriptor /* unused */) {
                      tensorpipe::Allocation allocation;
                      allocation.tensors.resize(1);
                      allocation.tensors[0].buffer = tensorpipe::CpuBuffer{
                          .ptr = slice.data_ptr(),
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
                            numMachinesDoneForBucket[bucketIdx] += 1;
                            if (numMachinesDoneForBucket[bucketIdx] <
                                numMachines_) {
                              return;
                            }
                            ncclBroadcastStartTimes_[bucketIdx] =
                                std::chrono::steady_clock::now();
                            NCCL_CHECK(ncclBroadcast(
                                stagingTensors_.value()[bucketIdx].data_ptr(),
                                buckets_[bucketIdx].data_ptr(),
                                bucketSize_,
                                ncclFloat,
                                0,
                                ncclComm_.get(),
                                stream));
                            broadcastEvents[bucketIdx].record(stream);
                            barrier.notifyExternalEventHappened();
                          }));
                    }));
              }));
        }
      }

      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        barrier.waitForNextExternalEvent();
        broadcastEvents[bucketIdx].synchronize();
        stats_.buckets[bucketIdx].ncclBroadcastTime = deltaAsUs(
            ncclBroadcastStartTimes_[bucketIdx],
            std::chrono::steady_clock::now());
      }

      barrier.join();
      stream.synchronize();
    } else {
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        NCCL_CHECK(ncclReduce(
            buckets_[bucketIdx].data_ptr(),
            nullptr,
            bucketSize_,
            ncclFloat,
            ncclSum,
            0,
            ncclComm_.get(),
            stream));
      }
      for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
        NCCL_CHECK(ncclBroadcast(
            nullptr,
            buckets_[bucketIdx].data_ptr(),
            bucketSize_,
            ncclFloat,
            0,
            ncclComm_.get(),
            stream));
      }

      stream.synchronize();
    }

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

} // namespace benchmark_herring_tcp
} // namespace

namespace py = pybind11;

template <typename T>
using shared_ptr_class_ = py::class_<T, std::shared_ptr<T>>;

PYBIND11_MODULE(benchmark_herring_tcp, module) {
  shared_ptr_class_<benchmark_herring_tcp::Server> server(module, "Server");
  shared_ptr_class_<benchmark_herring_tcp::Client> client(module, "Client");

  py::class_<
      benchmark_herring_tcp::ServerStats::EpochStats::BucketStats::MachineStats>
      serverStatsEpochBucketMachine(module, "ServerStatsEpochBucketMachine");
  serverStatsEpochBucketMachine.def_readonly(
      "addition_time",
      &benchmark_herring_tcp::ServerStats::EpochStats::BucketStats::
          MachineStats::additionTime);
  serverStatsEpochBucketMachine.def_readonly(
      "recv_to_send_time",
      &benchmark_herring_tcp::ServerStats::EpochStats::BucketStats::
          MachineStats::recvToSendTime);

  py::class_<benchmark_herring_tcp::ServerStats::EpochStats::BucketStats>
      serverStatsEpochBucket(module, "ServerStatsEpochBucket");
  serverStatsEpochBucket.def_readonly(
      "machines",
      &benchmark_herring_tcp::ServerStats::EpochStats::BucketStats::machines);

  py::class_<benchmark_herring_tcp::ServerStats::EpochStats> serverStatsEpoch(
      module, "ServerStatsEpoch");
  serverStatsEpoch.def_readonly(
      "buckets", &benchmark_herring_tcp::ServerStats::EpochStats::buckets);

  py::class_<benchmark_herring_tcp::ServerStats> serverStats(
      module, "ServerStats");
  serverStats.def_readonly(
      "epochs", &benchmark_herring_tcp::ServerStats::epochs);

  server.def(
      py::init<
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          size_t,
          const c10::intrusive_ptr<c10d::Store>&,
          size_t>(),
      py::arg("machine_idx"),
      py::arg("num_machines"),
      py::arg("num_devices_per_machine"),
      py::arg("num_buckets"),
      py::arg("bucket_size"),
      py::arg("num_epochs"),
      py::arg("store"),
      py::arg("num_threads"));
  server.def(
      "run",
      &benchmark_herring_tcp::Server::run,
      py::call_guard<py::gil_scoped_release>());

  py::class_<
      benchmark_herring_tcp::ClientStats::EpochStats::BucketStats::ServerStats>
      clientStatsEpochBucketMachine(module, "ClientStatsEpochBucketMachine");
  clientStatsEpochBucketMachine.def_readonly(
      "transfer_time",
      &benchmark_herring_tcp::ClientStats::EpochStats::BucketStats::
          ServerStats::transferTime);

  py::class_<benchmark_herring_tcp::ClientStats::EpochStats::BucketStats>
      clientStatsEpochBucket(module, "ClientStatsEpochBucket");
  clientStatsEpochBucket.def_readonly(
      "servers",
      &benchmark_herring_tcp::ClientStats::EpochStats::BucketStats::servers);
  clientStatsEpochBucket.def_readonly(
      "nccl_broadcast_time",
      &benchmark_herring_tcp::ClientStats::EpochStats::BucketStats::
          ncclBroadcastTime);
  clientStatsEpochBucket.def_readonly(
      "nccl_reduce_time",
      &benchmark_herring_tcp::ClientStats::EpochStats::BucketStats::
          ncclReduceTime);

  py::class_<benchmark_herring_tcp::ClientStats::EpochStats> clientStatsEpoch(
      module, "ClientStatsEpoch");
  clientStatsEpoch.def_readonly(
      "buckets", &benchmark_herring_tcp::ClientStats::EpochStats::buckets);
  clientStatsEpoch.def_readonly(
      "end_to_end_time",
      &benchmark_herring_tcp::ClientStats::EpochStats::endToEndTime);

  py::class_<benchmark_herring_tcp::ClientStats> clientStats(
      module, "ClientStats");
  clientStats.def_readonly(
      "epochs", &benchmark_herring_tcp::ClientStats::epochs);

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
      &benchmark_herring_tcp::Client::run,
      py::call_guard<py::gil_scoped_release>());
}
