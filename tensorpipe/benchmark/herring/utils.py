# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import multiprocessing
from typing import Any, List, Tuple


def recv_from_connections_and_join_processes(
    processes_and_connections: List[
        Tuple[multiprocessing.Process, multiprocessing.connection.Connection]
    ],
) -> List[Any]:
    """
    Wait for processes to return a value via a connection and then to terminate

    Given a list of processes and, for each of them, (the reading end of) a
    connection on which the process will send its result, gather the results of
    all processes and then join them, with extra care taken to handle any error
    (e.g., process crashing without returning) and kill all processes in case.
    """
    results = [None] * len(processes_and_connections)

    try:
        connections = [c for _, c in processes_and_connections]
        sentinels = [p.sentinel for p, _ in processes_and_connections]
        not_ready = connections + sentinels
        while len(not_ready) > 0:
            ready = multiprocessing.connection.wait(not_ready)
            for obj in ready:
                if obj in connections:
                    idx = connections.index(obj)
                    try:
                        val = obj.recv()
                    except EOFError:
                        # We won't get any more values out of this connection.
                        not_ready.remove(obj)
                    else:
                        if results[idx] is not None:
                            raise RuntimeError(
                                f"Process {idx} returned more than one value"
                            )
                        # Wrap in a tuple so we can distinguish a process that
                        # returned None from one that didn't return yet.
                        results[idx] = (val,)
                elif obj in sentinels:
                    idx = sentinels.index(obj)
                    proc, _ = processes_and_connections[idx]
                    proc.join()
                    if proc.exitcode != 0:
                        raise RuntimeError(
                            f"Process {idx} exited with status {proc.exitcode}"
                        )
                    not_ready.remove(obj)
                else:
                    raise RuntimeError(f"Unexpected object: {obj}")
    except Exception:
        for p, _ in processes_and_connections:
            p.kill()
        for p, _ in processes_and_connections:
            p.join()
        raise

    for idx, result in enumerate(results):
        if result is None:
            raise RuntimeError(f"Process {idx} exited without producing a result")

    # Unwrap from the tuples.
    return [r for r, in results]
