# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import argparse
import dataclasses
import multiprocessing
import os
import sys
from typing import List

import torch
import torch.distributed
from utils import recv_from_connections_and_join_processes

# Must come after torch or else it will fail because it won't find libc10.so
import benchmark_herring_tcp  # isort: skip


@dataclasses.dataclass
class ServerStats:
    addition_time: List[List[List[int]]]  # epoch x bucket x machine
    recv_to_send_time: List[List[List[int]]]  # epoch x bucket x machine


@dataclasses.dataclass
class ClientStats:
    transfer_time: List[List[List[int]]]  # epoch x bucket x server
    nccl_reduce_time: List[List[int]]  # epoch x bucket
    nccl_broadcast_time: List[List[int]]  # epoch x bucket
    end_to_end_time: List[int]  # epoch


@dataclasses.dataclass
class OneMachineHerringStats:
    addition_time: torch.Tensor  # epoch x bucket x machine
    recv_to_send_time: torch.Tensor  # epoch x bucket x machine
    transfer_time: torch.Tensor  # client x epoch x bucket x server
    nccl_reduce_time: torch.Tensor  # client x epoch x bucket
    nccl_broadcast_time: torch.Tensor  # client x epoch x bucket
    end_to_end_time: torch.Tensor  # client x epoch


def run_herring_server(
    init_method: str,
    machine_idx: int,
    num_machines: int,
    num_devices_per_machine: int,
    num_buckets: int,
    bucket_size: int,
    num_epochs: int,
    num_compute_threads: int,
    conn: multiprocessing.connection.Connection,
) -> None:
    torch._C._set_print_stack_traces_on_fatal_signal(True)

    rdv_iterator = torch.distributed.rendezvous(
        init_method,
        num_machines * num_devices_per_machine + machine_idx,
        num_machines * num_devices_per_machine + num_machines,
    )
    store, _, _ = next(rdv_iterator)

    assert 0 <= machine_idx < num_machines

    server = benchmark_herring_tcp.Server(
        machine_idx=machine_idx,
        num_machines=num_machines,
        num_devices_per_machine=num_devices_per_machine,
        num_buckets=num_buckets,
        bucket_size=bucket_size,
        num_epochs=num_epochs,
        store=store,
        num_threads=num_compute_threads,
    )
    stats = server.run()
    conn.send(
        ServerStats(
            addition_time=[
                [[ms.addition_time for ms in bs.machines] for bs in es.buckets]
                for es in stats.epochs
            ],
            recv_to_send_time=[
                [[ms.recv_to_send_time for ms in bs.machines] for bs in es.buckets]
                for es in stats.epochs
            ],
        )
    )


def run_herring_client(
    init_method: str,
    machine_idx: int,
    device_idx: int,
    num_machines: int,
    num_devices_per_machine: int,
    num_buckets: int,
    bucket_size: int,
    num_epochs: int,
    conn: multiprocessing.connection.Connection,
) -> None:
    torch._C._set_print_stack_traces_on_fatal_signal(True)

    rdv_iterator = torch.distributed.rendezvous(
        init_method,
        machine_idx * num_devices_per_machine + device_idx,
        num_machines * num_devices_per_machine + num_machines,
    )
    store, _, _ = next(rdv_iterator)

    assert 0 <= machine_idx < num_machines
    assert 0 <= device_idx < num_devices_per_machine

    os.environ["CUDA_VISIBLE_DEVICES"] = f"{device_idx}"

    client = benchmark_herring_tcp.Client(
        machine_idx=machine_idx,
        device_idx=device_idx,
        num_machines=num_machines,
        num_devices_per_machine=num_devices_per_machine,
        num_buckets=num_buckets,
        bucket_size=bucket_size,
        num_epochs=num_epochs,
        store=store,
    )
    stats = client.run()
    conn.send(
        ClientStats(
            transfer_time=[
                [[ss.transfer_time for ss in bs.servers] for bs in es.buckets]
                for es in stats.epochs
            ],
            nccl_reduce_time=[
                [bs.nccl_reduce_time for bs in es.buckets] for es in stats.epochs
            ],
            nccl_broadcast_time=[
                [bs.nccl_broadcast_time for bs in es.buckets] for es in stats.epochs
            ],
            end_to_end_time=[es.end_to_end_time for es in stats.epochs],
        )
    )


def run_one_machine_herring(
    init_method: str,
    machine_idx: int,
    num_machines: int,
    num_devices_per_machine: int,
    num_buckets: int,
    bucket_size: int,
    num_epochs: int,
    num_compute_threads: int,
    num_network_threads: int,
) -> OneMachineHerringStats:
    server_receiving_conn, server_sending_conn = multiprocessing.Pipe()
    server = multiprocessing.Process(
        target=run_herring_server,
        name=f"server_{machine_idx}",
        args=(
            init_method,
            machine_idx,
            num_machines,
            num_devices_per_machine,
            num_buckets,
            bucket_size,
            num_epochs,
            num_compute_threads,
            server_sending_conn,
        ),
    )

    client_receiving_conns = []
    client_sending_conns = []
    for _ in range(num_devices_per_machine):
        recv_end, send_end = multiprocessing.Pipe()
        client_receiving_conns.append(recv_end)
        client_sending_conns.append(send_end)
    clients = [
        multiprocessing.Process(
            target=run_herring_client,
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
                client_sending_conns[device_idx],
            ),
        )
        for device_idx in range(num_devices_per_machine)
    ]
    for t in [server] + clients:
        t.start()
    for c in [server_sending_conn] + client_sending_conns:
        c.close()

    stats = recv_from_connections_and_join_processes(
        [(server, server_receiving_conn)] + list(zip(clients, client_receiving_conns))
    )
    server_stats = stats[0]
    client_stats = stats[1:]

    return OneMachineHerringStats(
        addition_time=torch.tensor(
            server_stats.addition_time,
            dtype=torch.long,
        ),
        recv_to_send_time=torch.tensor(
            server_stats.recv_to_send_time,
            dtype=torch.long,
        ),
        transfer_time=torch.tensor(
            [s.transfer_time for s in client_stats],
            dtype=torch.long,
        ),
        nccl_reduce_time=torch.tensor(
            [s.nccl_reduce_time for s in client_stats],
            dtype=torch.long,
        ),
        nccl_broadcast_time=torch.tensor(
            [s.nccl_broadcast_time for s in client_stats],
            dtype=torch.long,
        ),
        end_to_end_time=torch.tensor(
            [s.end_to_end_time for s in client_stats],
            dtype=torch.long,
        ),
    )


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
        "--num-compute-threads",
        type=int,
        required=True,
        help="How many threads to use to calculate reductions on the servers",
    )
    parser.add_argument(
        "--num-network-threads",
        type=int,
        required=True,
        help="How many TCP event loop threads to use (to multiplex and saturate bandwidth)",
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
    )

    args = parser.parse_args()

    res = run_one_machine_herring(
        init_method=args.init_method,
        machine_idx=args.machine_idx,
        num_machines=args.num_machines,
        num_devices_per_machine=args.num_devices_per_machine,
        num_buckets=args.num_buckets,
        bucket_size=args.bucket_size,
        num_epochs=args.num_epochs,
        num_compute_threads=args.num_compute_threads,
        num_network_threads=args.num_network_threads,
    )

    torch.save(res, args.output)


if __name__ == "__main__":
    main()
