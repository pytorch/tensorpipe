#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import subprocess
import sys
from pathlib import Path

from tools.hipify import hipify_python
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from typing import Optional

def _find_rocm_home() -> Optional[str]:
    r'''Finds the ROCm install path.'''
    # Guess #1
    rocm_home = os.environ.get('ROCM_HOME') or os.environ.get('ROCM_PATH')
    if rocm_home is None:
        # Guess #2
        try:
            hipcc = subprocess.check_output(
                ['which', 'hipcc'], stderr=subprocess.DEVNULL).decode().rstrip('\r\n')
            # this will be either <ROCM_HOME>/hip/bin/hipcc or <ROCM_HOME>/bin/hipcc
            rocm_home = os.path.dirname(os.path.dirname(hipcc))
            if os.path.basename(rocm_home) == 'hip':
                rocm_home = os.path.dirname(rocm_home)
        except Exception:
            # Guess #3
            rocm_home = '/opt/rocm'
            if not os.path.exists(rocm_home):
                rocm_home = None
    if rocm_home is None:
        print(f"No ROCm runtime is found, using ROCM_HOME='{rocm_home}'")
    return rocm_home

ROCM_HOME = _find_rocm_home()
BUILD_FOR_ROCM = True if (ROCM_HOME is not None) else False

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

        if BUILD_FOR_ROCM:
            hipify_result = hipify_python.hipify(
                    project_directory=source_path,
                    output_directory=source_path,
                    includes=['*'],
                    is_pytorch_extension=True)

            print(hipify_result)

        cmake_cmd = [
            "cmake",
            f"{source_path}",
#            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={output_path}",
#            f"-DPYTHON_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={build_type}",
            "-DCMAKE_C_COMPILER=gcc",
            "-DCMAKE_CXX_COMPILER=g++",
            "-DTP_USE_ROCM=BUILD_FOR_ROCM",
#            "-DCMAKE_POSITION_INDEPENDENT_CODE=true",
#            "-DTP_BUILD_PYTHON=true",
        ]

        for opt in os.environ:
            if opt.startswith("TP_"):
                cmake_cmd.append(f"-D{opt}={os.environ[opt]}")

#        make_cmd = ["make", "-j", "pytensorpipe"]
        make_cmd = ["make"]

#        subprocess.check_call(cmake_cmd, cwd=self.build_temp)
#        subprocess.check_call(make_cmd, cwd=self.build_temp)


setup(
    name="tensorpipe",
    version="0.0.0",
    author="Facebook AI Research",
    ext_modules=[Extension("pytensorpipe", sources=[])],
    cmdclass={"build_ext": CMakeBuild},
    zip_safe=False,
)
