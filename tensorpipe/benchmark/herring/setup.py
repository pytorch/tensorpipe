#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from setuptools import setup
from torch.utils import cpp_extension

setup(
    name="herring",
    ext_modules=[
        cpp_extension.CUDAExtension(
            "benchmark_herring_gdr",
            [
                "benchmark_herring_gdr.cc",
                "cuda_kernels.cu",
            ],
            libraries=["nccl", "tensorpipe", "tensorpipe_cuda"],
        ),
        cpp_extension.CUDAExtension(
            "benchmark_herring_tcp",
            [
                "benchmark_herring_tcp.cc",
            ],
            libraries=["nccl", "tensorpipe"],
        ),
        cpp_extension.CUDAExtension(
            "benchmark_nccl",
            [
                "benchmark_nccl.cc",
            ],
            libraries=["nccl"],
        ),
    ],
    py_modules=[
        "launch_herring_gdr",
        "launch_herring_tcp",
        "launch_nccl",
        "utils",
    ],
    cmdclass={"build_ext": cpp_extension.BuildExtension},
    entry_points={
        "console_scripts": [
            "launch_herring_gdr=launch_herring_gdr:main",
            "launch_herring_tcp=launch_herring_tcp:main",
            "launch_nccl=launch_nccl:main",
        ],
    },
    setup_requires=["setuptools", "torch"],
    install_requires=["torch"],
)
