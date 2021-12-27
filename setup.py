#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import subprocess
import sys
from pathlib import Path

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class CMakeBuild(build_ext):
    def run(self):
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        source_path = Path(__file__).parent.resolve()
        output_path = Path(self.get_ext_fullpath(ext.name)).parent.resolve()
        build_type = "Debug" if self.debug else "Release"

        cmake_cmd = [
            "cmake",
            f"{source_path}",
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={output_path}",
            f"-DPYTHON_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={build_type}",
            "-DCMAKE_C_COMPILER=clang-6.0",
            "-DCMAKE_CXX_COMPILER=clang++-6.0",
            "-DCMAKE_POSITION_INDEPENDENT_CODE=true",
            "-DTP_BUILD_PYTHON=true",
        ]

        for opt in os.environ:
            if opt.startswith("TP_"):
                cmake_cmd.append(f"-D{opt}={os.environ[opt]}")

        make_cmd = ["make", "-j", "pytensorpipe"]

        subprocess.check_call(cmake_cmd, cwd=self.build_temp)
        subprocess.check_call(make_cmd, cwd=self.build_temp)


setup(
    name="tensorpipe",
    version="0.0.0",
    author="Facebook AI Research",
    ext_modules=[Extension("pytensorpipe", sources=[])],
    cmdclass={"build_ext": CMakeBuild},
    zip_safe=False,
)
