#!/usr/bin/env python

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import sys
import argparse
import json

parser = argparse.ArgumentParser(description="")
parser.add_argument(
    '--io-file',
    type=str,
    help="Input file to read the list of files",
    required=True)

parser.add_argument(
    '--dump-dict-directory',
    type=str,
    help="The Directory when the dictionary output of hipified is stored",
    required=True)

args = parser.parse_args()

dict_file_name = args.dump_dict_directory + "/hipify_output_dict_dump.txt"
file_obj = open(dict_file_name, mode='r')
json_string = file_obj.read()
file_obj.close()
hipfied_result = json.loads(json_string)

out_list = []
with open(args.io_file) as inp_file:
    for line in inp_file:
        line = line.strip()
        if line in hipfied_result:
            out_list.append(hipfied_result[line]['hipified_path'])
        else:
            out_list.append(line)

w_file_obj = open(args.io_file, mode='w')
for f in out_list:
    w_file_obj.write(f+"\n")
w_file_obj.close()
