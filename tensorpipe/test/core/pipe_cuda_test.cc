/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/core/pipe_test.h>

using namespace tensorpipe;

class CudaSimpleWriteReadWithAllTargetDevicesTest
    : public ClientServerPipeTestCase {
  InlineMessage imessage_ = {
      .payloads =
          {
              {.data = "payload #1", .metadata = "payload metadata #1"},
              {.data = "payload #2", .metadata = "payload metadata #2"},
              {.data = "payload #3", .metadata = "payload metadata #3"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1",
                  .metadata = "tensor metadata #1",
                  .device = Device{kCudaDeviceType, 0},
                  .targetDevice = Device{kCudaDeviceType, 0},
              },
              {
                  .data = "tensor #2",
                  .metadata = "tensor metadata #2",
                  .device = Device{kCpuDeviceType, 0},
                  .targetDevice = Device{kCudaDeviceType, 0},
              },
              {
                  .data = "tensor #3",
                  .metadata = "tensor metadata #3",
                  .device = Device{kCudaDeviceType, 0},
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #4",
                  .metadata = "tensor metadata #4",
                  .device = Device{kCpuDeviceType, 0},
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "pipe metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message;
    Storage storage;
    std::tie(message, storage) = makeMessage(imessage_);
    auto future = pipeWriteWithFuture(pipe, message);
    future.get();
  }

  void client(Pipe& pipe) override {
    Descriptor descriptor;
    Storage storage;
    auto future = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCudaDeviceType, 0},
            Device{kCudaDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, CudaSimpleWriteReadWithAllTargetDevices) {
  CudaSimpleWriteReadWithAllTargetDevicesTest test;
  test.run();
}

class CudaSimpleWriteReadWithSomeTargetDevicesTest
    : public ClientServerPipeTestCase {
  InlineMessage imessage_ = {
      .payloads =
          {
              {.data = "payload #1", .metadata = "payload metadata #1"},
              {.data = "payload #2", .metadata = "payload metadata #2"},
              {.data = "payload #3", .metadata = "payload metadata #3"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1",
                  .metadata = "tensor metadata #1",
                  .device = Device{kCudaDeviceType, 0},
                  .targetDevice = Device{kCudaDeviceType, 0},
              },
              {
                  .data = "tensor #2",
                  .metadata = "tensor metadata #2",
                  .device = Device{kCudaDeviceType, 0},
              },
              {
                  .data = "tensor #3",
                  .metadata = "tensor metadata #3",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "pipe metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message;
    Storage storage;
    std::tie(message, storage) = makeMessage(imessage_);
    auto future = pipeWriteWithFuture(pipe, message);
    future.get();
  }

  void client(Pipe& pipe) override {
    Descriptor descriptor;
    Storage storage;
    auto future = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCudaDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, CudaSimpleWriteReadWithSomeTargetDevices) {
  CudaSimpleWriteReadWithSomeTargetDevicesTest test;
  test.run();
}

class CudaSimpleWriteReadWithoutTargetDeviceTest
    : public ClientServerPipeTestCase {
  InlineMessage imessage_ = {
      .payloads =
          {
              {.data = "payload #1", .metadata = "payload metadata #1"},
              {.data = "payload #2", .metadata = "payload metadata #2"},
              {.data = "payload #3", .metadata = "payload metadata #3"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1",
                  .metadata = "tensor metadata #1",
                  .device = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #2",
                  .metadata = "tensor metadata #2",
                  .device = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #3",
                  .metadata = "tensor metadata #3",
                  .device = Device{kCudaDeviceType, 0},
              },
              {
                  .data = "tensor #4",
                  .metadata = "tensor metadata #4",
                  .device = Device{kCudaDeviceType, 0},
              },
          },
      .metadata = "pipe metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message;
    Storage storage;
    std::tie(message, storage) = makeMessage(imessage_);
    auto future = pipeWriteWithFuture(pipe, message);
    future.get();
  }

  void client(Pipe& pipe) override {
    Descriptor descriptor;
    Storage storage;
    auto future = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
            Device{kCudaDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCudaDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, CudaSimpleWriteReadWithoutTargetDevice) {
  CudaSimpleWriteReadWithoutTargetDeviceTest test;
  test.run();
}
