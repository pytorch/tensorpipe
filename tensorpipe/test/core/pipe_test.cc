/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/test/core/pipe_test.h>

using namespace tensorpipe;

class SimpleWriteReadTest : public ClientServerPipeTestCase {
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
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, SimpleWriteRead) {
  SimpleWriteReadTest test;
  test.run();
}

class SimpleWriteReadPayloadsOnlyTest : public ClientServerPipeTestCase {
  InlineMessage imessage_ = {
      .payloads =
          {
              {.data = "payload #1", .metadata = "payload metadata #1"},
              {.data = "payload #2", .metadata = "payload metadata #2"},
              {.data = "payload #3", .metadata = "payload metadata #3"},
          },
      .tensors = {},
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
    auto future = pipeReadWithFuture(pipe, /*targetDevices=*/{});
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, SimpleWriteReadPayloadsOnly) {
  SimpleWriteReadPayloadsOnlyTest test;
  test.run();
}

class SimpleWriteReadTensorsOnlyTest : public ClientServerPipeTestCase {
  InlineMessage imessage_ = {
      .payloads = {},
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
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, SimpleWriteReadTensorsOnly) {
  SimpleWriteReadTensorsOnlyTest test;
  test.run();
}

class SimpleWriteReadWithAllTargetDevicesTest
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
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #2",
                  .metadata = "tensor metadata #2",
                  .device = Device{kCpuDeviceType, 0},
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #3",
                  .metadata = "tensor metadata #3",
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
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, SimpleWriteReadWithAllTargetDevices) {
  SimpleWriteReadWithAllTargetDevicesTest test;
  test.run();
}

class SimpleWriteReadWithSomeTargetDevicesTest
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
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #2",
                  .metadata = "tensor metadata #2",
                  .device = Device{kCpuDeviceType, 0},
              },
              {
                  .data = "tensor #3",
                  .metadata = "tensor metadata #3",
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
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
            Device{kCpuDeviceType, 0},
        });
    std::tie(descriptor, storage) = future.get();
    expectDescriptorAndStorageMatchMessage(descriptor, storage, imessage_);
  }
};

TEST(Pipe, SimpleWriteReadWithSomeTargetDevices) {
  SimpleWriteReadWithSomeTargetDevicesTest test;
  test.run();
}

class MultipleWriteReadTest : public ClientServerPipeTestCase {
  InlineMessage imessage1_ = {
      .payloads =
          {
              {.data = "payload #1.1", .metadata = "payload metadata #1.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1.1",
                  .metadata = "tensor metadata #1.1",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

  InlineMessage imessage2_ = {
      .payloads =
          {
              {.data = "payload #2.1", .metadata = "payload metadata #2.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #2.1",
                  .metadata = "tensor metadata #2.1",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message1;
    Storage storage1;
    std::tie(message1, storage1) = makeMessage(imessage1_);
    auto future1 = pipeWriteWithFuture(pipe, message1);

    Message message2;
    Storage storage2;
    std::tie(message2, storage2) = makeMessage(imessage2_);
    auto future2 = pipeWriteWithFuture(pipe, message2);

    future1.get();
    future2.get();
  }

  void client(Pipe& pipe) override {
    auto future1 = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });
    auto future2 = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });

    Descriptor descriptor1;
    Storage storage1;
    std::tie(descriptor1, storage1) = future1.get();
    expectDescriptorAndStorageMatchMessage(descriptor1, storage1, imessage1_);

    Descriptor descriptor2;
    Storage storage2;
    std::tie(descriptor2, storage2) = future2.get();
    expectDescriptorAndStorageMatchMessage(descriptor2, storage2, imessage2_);
  }
};

TEST(Pipe, MultipleWriteRead) {
  MultipleWriteReadTest test;
  test.run();
}

class MultipleWriteReadWithSomeTargetDevicesTest
    : public ClientServerPipeTestCase {
  InlineMessage imessage1_ = {
      .payloads =
          {
              {.data = "payload #1.1", .metadata = "payload metadata #1.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1.1",
                  .metadata = "tensor metadata #1.1",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

  InlineMessage imessage2_ = {
      .payloads =
          {
              {.data = "payload #2.1", .metadata = "payload metadata #2.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #2.1",
                  .metadata = "tensor metadata #2.1",
                  .device = Device{kCpuDeviceType, 0},
                  .targetDevice = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message1;
    Storage storage1;
    std::tie(message1, storage1) = makeMessage(imessage1_);
    auto future1 = pipeWriteWithFuture(pipe, message1);

    Message message2;
    Storage storage2;
    std::tie(message2, storage2) = makeMessage(imessage2_);
    auto future2 = pipeWriteWithFuture(pipe, message2);

    future1.get();
    future2.get();
  }

  void client(Pipe& pipe) override {
    auto future1 = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });
    auto future2 = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });

    Descriptor descriptor1;
    Storage storage1;
    std::tie(descriptor1, storage1) = future1.get();
    expectDescriptorAndStorageMatchMessage(descriptor1, storage1, imessage1_);

    Descriptor descriptor2;
    Storage storage2;
    std::tie(descriptor2, storage2) = future2.get();
    expectDescriptorAndStorageMatchMessage(descriptor2, storage2, imessage2_);
  }
};

TEST(Pipe, MultipleWriteReadWithSomeTargetDevices) {
  MultipleWriteReadWithSomeTargetDevicesTest test;
  test.run();
}

class WriteFromBothThenReadTest : public ClientServerPipeTestCase {
  InlineMessage imessage1_ = {
      .payloads =
          {
              {.data = "payload #1.1", .metadata = "payload metadata #1.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #1.1",
                  .metadata = "tensor metadata #1.1",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

  InlineMessage imessage2_ = {
      .payloads =
          {
              {.data = "payload #2.1", .metadata = "payload metadata #2.1"},
          },
      .tensors =
          {
              {
                  .data = "tensor #2.1",
                  .metadata = "tensor metadata #2.1",
                  .device = Device{kCpuDeviceType, 0},
              },
          },
      .metadata = "message metadata",
  };

 public:
  void server(Pipe& pipe) override {
    Message message;
    Storage writeStorage;
    std::tie(message, writeStorage) = makeMessage(imessage1_);
    auto writeFuture = pipeWriteWithFuture(pipe, message);

    auto readFuture = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });

    writeFuture.get();

    Descriptor descriptor;
    Storage readStorage;
    std::tie(descriptor, readStorage) = readFuture.get();
    expectDescriptorAndStorageMatchMessage(descriptor, readStorage, imessage2_);
  }

  void client(Pipe& pipe) override {
    Message message;
    Storage writeStorage;
    std::tie(message, writeStorage) = makeMessage(imessage2_);
    auto writeFuture = pipeWriteWithFuture(pipe, message);

    auto readFuture = pipeReadWithFuture(
        pipe,
        /*targetDevices=*/
        {
            Device{kCpuDeviceType, 0},
        });

    writeFuture.get();

    Descriptor descriptor;
    Storage readStorage;
    std::tie(descriptor, readStorage) = readFuture.get();
    expectDescriptorAndStorageMatchMessage(descriptor, readStorage, imessage1_);
  }
};

TEST(Pipe, WriteFromBothThenRead) {
  WriteFromBothThenReadTest test;
  test.run();
}
