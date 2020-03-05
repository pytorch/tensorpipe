#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
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
        context.register_transport(0, "tcp", tp.UvTransport())
        context.register_channel(0, "basic", tp.BasicChannel())
        context.register_channel(-1, "intra_process", tp.IntraProcessChannel())

        # We must keep a reference to it, or it will be destroyed early.
        server_pipe = None

        listener = tp.Listener(context, ["tcp://127.0.0.1"])

        def on_connection(pipe: tp.Pipe) -> None:
            global server_pipe
            tensor = tp.OutgoingTensor(b"World!", b"")
            message = tp.OutgoingMessage(b"Hello ", b"", [tensor])
            pipe.write(message, on_write)
            server_pipe = pipe

        def on_write() -> None:
            pass

        listener.listen(on_connection)

        client_pipe = tp.Pipe(context, listener.get_url("tcp"))

        received_message = None
        received_tensors = None
        read_completed = threading.Event()

        def on_read_descriptor(message: tp.IncomingMessage) -> None:
            nonlocal received_message, received_tensors
            received_message = bytearray(message.length)
            message.buffer = received_message
            received_tensors = []
            for tensor in message.tensors:
                received_tensors.append(bytearray(tensor.length))
                tensor.buffer = received_tensors[-1]
            client_pipe.read(message, on_read)

        def on_read() -> None:
            read_completed.set()

        client_pipe.read_descriptor(on_read_descriptor)

        read_completed.wait()

        self.assertEqual(received_message, bytearray(b"Hello "))
        self.assertEqual(received_tensors, [bytearray(b"World!")])

        # del client_pipe
        # del server_pipe
        # del listener

        # context.join()


if __name__ == "__main__":
    unittest.main()
