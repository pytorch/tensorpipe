# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import multiprocessing
import os
import sys
from typing import List, Optional

import torch
import torch.distributed
from utils import recv_from_connections_and_join_processes

# Must come after torch or else it will fail because it won't find libc10.so
import benchmark_nccl  # isort: skip


def run_nccl_client(
    init_method: str,
    machine_idx: int,
    device_idx: int,
    num_machines: int,
    num_devices_per_machine: int,
    num_buckets: int,
    bucket_size: int,
    num_epochs: int,
    num_network_threads: Optional[int],
    num_sockets_per_network_thread: Optional[int],
    conn: multiprocessing.connection.Connection,
) -> List[int]:
    torch._C._set_print_stack_traces_on_fatal_signal(True)

    rdv_iterator = torch.distributed.rendezvous(
        init_method,
        machine_idx * num_devices_per_machine + device_idx,
        num_machines * num_devices_per_machine,
    )
    store, _, _ = next(rdv_iterator)

    assert 0 <= machine_idx < num_machines
    assert 0 <= device_idx < num_devices_per_machine

    os.environ["CUDA_VISIBLE_DEVICES"] = f"{device_idx}"
    if num_network_threads is not None:
        os.environ["NCCL_SOCKET_NTHREADS"] = f"{num_network_threads}"
    if num_sockets_per_network_thread is not None:
        os.environ["NCCL_NSOCKS_PERTHREAD"] = f"{num_sockets_per_network_thread}"

    client = benchmark_nccl.Client(
        machine_idx=machine_idx,
        device_idx=device_idx,
        num_machines=num_machines,
        num_devices_per_machine=num_devices_per_machine,
        num_buckets=num_buckets,
        bucket_size=bucket_size,
        num_epochs=num_epochs,
        store=store,
    )
    conn.send(client.run())


def run_one_machine_nccl(
    init_method: str,
    machine_idx: int,
    num_machines: int,
    num_devices_per_machine: int,
    num_buckets: int,
    bucket_size: int,
    num_epochs: int,
    num_network_threads: Optional[int],
    num_sockets_per_network_thread: Optional[int],
) -> torch.Tensor:
    receiving_conns = []
    sending_conns = []
    for _ in range(num_devices_per_machine):
        recv_end, send_end = multiprocessing.Pipe()
        receiving_conns.append(recv_end)
        sending_conns.append(send_end)
    clients = [
        multiprocessing.Process(
            target=run_nccl_client,
            name=f"client_{machine_idx}_{device_idx}",
            args=(
                init_method,
                machine_idx,
                device_idx,
                num_machines,
                num_devices_per_machine,
                num_buckets,
                bucket_size,
                num_epochs,
                num_network_threads,
                num_sockets_per_network_thread,
                sending_conns[device_idx],
            ),
        )
        for device_idx in range(num_devices_per_machine)
    ]
    for t in clients:
        t.start()
    for c in sending_conns:
        c.close()

    stats = recv_from_connections_and_join_processes(
        list(zip(clients, receiving_conns))
    )

    return torch.tensor(stats, dtype=torch.long)


def main():
    parser = argparse.ArgumentParser(description="NCCL allreduce benchmark")
    parser.add_argument(
        "--init-method",
        type=str,
        default="env://",
        help="How to do rendezvous between machines (uses PyTorch, hence see its doc)",
    )
    parser.add_argument(
        "--machine-idx",
        type=int,
        required=True,
        help="The rank of the machine on which this script was invoked (0-based)",
    )
    parser.add_argument(
        "--num-machines",
        type=int,
        required=True,
        help="On how many machines this script is being invoked (each with its own rank)",
    )
    parser.add_argument(
        "--num-devices-per-machine",
        type=int,
        required=True,
        help="How many clients this script should launch (each will use one GPU)",
    )
    parser.add_argument(
        "--num-buckets",
        type=int,
        required=True,
        help="How many buffers to do an allreduce over in each epoch",
    )
    parser.add_argument(
        "--bucket-size",
        type=int,
        required=True,
        help="How big each buffer should be (expressed in number of float32 elements)",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        required=True,
        help="How many times to run the benchmark",
    )
    parser.add_argument(
        "--num-network-threads",
        type=int,
        help="The value of the NCCL_SOCKET_NTHREADS env var (see NCCL's doc)",
    )
    parser.add_argument(
        "--num-sockets-per-network-thread",
        type=int,
        help="The value of the NCCL_NSOCKS_PERTHREAD env var (see NCCL's doc)",
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
    )

    args = parser.parse_args()

    res = run_one_machine_nccl(
        init_method=args.init_method,
        machine_idx=args.machine_idx,
        num_machines=args.num_machines,
        num_devices_per_machine=args.num_devices_per_machine,
        num_buckets=args.num_buckets,
        bucket_size=args.bucket_size,
        num_epochs=args.num_epochs,
        num_network_threads=args.num_network_threads,
        num_sockets_per_network_thread=args.num_sockets_per_network_thread,
    )

    torch.save(res, args.output)


if __name__ == "__main__":
    main()
