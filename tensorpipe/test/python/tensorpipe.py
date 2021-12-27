#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import threading
import unittest

import pytensorpipe as tp


class TestTensorpipe(unittest.TestCase):
    def test_read_write(self):
        context = tp.Context()
        context.register_transport(0, "tcp", tp.create_uv_transport())
        create_shm_transport = getattr(tp, "create_shm_transport", None)
        if create_shm_transport is not None:
            context.register_transport(-1, "shm", create_shm_transport())
        context.register_channel(0, "basic", tp.create_basic_channel())
        create_cma_channel = getattr(tp, "create_cma_channel", None)
        if create_cma_channel is not None:
            context.register_channel(-1, "cma", create_cma_channel())

        # We must keep a reference to it, or it will be destroyed early.
        server_pipe = None

        listener: tp.Listener = context.listen(["tcp://127.0.0.1"])

        write_completed = threading.Event()

        def on_connection(pipe: tp.Pipe) -> None:
            global server_pipe
            payload = tp.OutgoingPayload(b"Hello ", b"a greeting")
            tensor = tp.OutgoingTensor(b"World!", b"a place")
            message = tp.OutgoingMessage(b"metadata", [payload], [tensor])
            pipe.write(message, on_write)
            server_pipe = pipe

        def on_write() -> None:
            write_completed.set()

        listener.listen(on_connection)

        client_pipe: tp.Pipe = context.connect(listener.get_url("tcp"))

        received_payloads = None
        received_tensors = None
        read_completed = threading.Event()

        def on_read_descriptor(message: tp.IncomingMessage) -> None:
            nonlocal received_payloads, received_tensors
            self.assertEqual(message.metadata, bytearray(b"metadata"))
            received_payloads = []
            for payload in message.payloads:
                self.assertEqual(payload.metadata, bytearray(b"a greeting"))
                received_payloads.append(bytearray(payload.length))
                payload.buffer = received_payloads[-1]
            received_tensors = []
            for tensor in message.tensors:
                self.assertEqual(tensor.metadata, bytearray(b"a place"))
                received_tensors.append(bytearray(tensor.length))
                tensor.buffer = received_tensors[-1]
            client_pipe.read(message, on_read)

        def on_read() -> None:
            read_completed.set()

        client_pipe.read_descriptor(on_read_descriptor)

        write_completed.wait()
        read_completed.wait()

        self.assertEqual(received_payloads, [bytearray(b"Hello ")])
        self.assertEqual(received_tensors, [bytearray(b"World!")])

        # Due to a current limitation we're not releasing the GIL when calling
        # the context's destructor, which implicitly calls join, which may fire
        # some callbacks that also try to acquire the GIL and thus deadlock.
        # So, for now, we must explicitly call join.
        # See https://github.com/pybind/pybind11/issues/1446.
        context.join()


if __name__ == "__main__":
    unittest.main()
