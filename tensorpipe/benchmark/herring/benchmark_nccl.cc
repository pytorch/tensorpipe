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
#include <torch/csrc/utils/pybind.h>
#include <torch/torch.h>

namespace {

int64_t deltaAsUs(
    std::chrono::steady_clock::time_point start,
    std::chrono::steady_clock::time_point stop) {
  return std::chrono::duration_cast<std::chrono::microseconds>(stop - start)
      .count();
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

// We need this extra named namespace inside our unnamed namespace because of
// https://github.com/pybind/pybind11/issues/3289
namespace benchmark_nccl {

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
        store_(std::move(store)) {}

  std::vector<int64_t> run() {
    allocateTensors();
    setUpNccl();
    std::vector<int64_t> stats;
    for (size_t epochIdx = 0; epochIdx < numEpochs_; epochIdx += 1) {
      setTensorsToOne();
      {
        auto start = std::chrono::steady_clock::now();
        runOneEpoch();
        auto stop = std::chrono::steady_clock::now();
        stats.push_back(deltaAsUs(start, stop));
      }
      checkTensors();
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
  std::vector<torch::Tensor> buckets_;
  NcclComm ncclComm_;

  void allocateTensors() {
    buckets_.reserve(numBuckets_);
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_.push_back(torch::empty(
          bucketSize_,
          c10::TensorOptions()
              .dtype(c10::kFloat)
              .device(c10::Device(c10::kCUDA, 0))));
    }
  }

  void setUpNccl() {
    ncclUniqueId uniqueId;
    if (machineIdx_ == 0 && deviceIdx_ == 0) {
      NCCL_CHECK(ncclGetUniqueId(&uniqueId));
      store_->set(
          "nccl_id",
          std::vector<uint8_t>(
              reinterpret_cast<uint8_t*>(&uniqueId),
              reinterpret_cast<uint8_t*>(&uniqueId) + sizeof(ncclUniqueId)));
    } else {
      std::vector<uint8_t> uniqueIdData = store_->get("nccl_id");
      std::memcpy(&uniqueId, uniqueIdData.data(), sizeof(ncclUniqueId));
    }
    ncclComm_ = createNcclComm(
        /*rank=*/machineIdx_ * numDevicesPerMachine_ + deviceIdx_,
        /*worldSize=*/numMachines_ * numDevicesPerMachine_,
        uniqueId);
  }

  void setTensorsToOne() {
    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      buckets_[bucketIdx].fill_(1);
    }
  }

  void runOneEpoch() {
    c10::cuda::CUDAStream stream =
        c10::cuda::getStreamFromPool(/*isHighPriority=*/true, /*device=*/0);

    for (size_t bucketIdx = 0; bucketIdx < numBuckets_; bucketIdx += 1) {
      NCCL_CHECK(ncclAllReduce(
          buckets_[bucketIdx].data_ptr(),
          buckets_[bucketIdx].data_ptr(),
          bucketSize_,
          ncclFloat,
          ncclSum,
          ncclComm_.get(),
          stream));
    }

    stream.synchronize();
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

} // namespace benchmark_nccl
} // namespace

namespace py = pybind11;

template <typename T>
using shared_ptr_class_ = py::class_<T, std::shared_ptr<T>>;

PYBIND11_MODULE(benchmark_nccl, module) {
  shared_ptr_class_<benchmark_nccl::Client> client(module, "Client");

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
      &benchmark_nccl::Client::run,
      py::call_guard<py::gil_scoped_release>());
}
