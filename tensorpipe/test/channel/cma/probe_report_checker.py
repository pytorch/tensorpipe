#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import sys

if __name__ == "__main__":
    if len(sys.argv) < 1:
        raise RuntimeError()
    if len(sys.argv) != 4:
        print(
            f"Usage: {sys.argv[0]} [first report] [second report] [supposed to work]",
            file=sys.stderr,
        )
        sys.exit(0)

    with open(sys.argv[1], "rb") as f:
        first_report = json.load(f)
    with open(sys.argv[2], "rb") as f:
        second_report = json.load(f)
    supposed_to_work = int(sys.argv[3])

    worked_in_practice = (
        first_report["syscall_success"] == 1 and second_report["syscall_success"] == 1
    )
    if worked_in_practice != supposed_to_work:
        raise RuntimeError(
            f"The syscall didn't behave as the test expected it to. It "
            f"{'succeeded' if worked_in_practice else 'failed'} whereas it was "
            f"supposed to {'succeed' if supposed_to_work else 'fail'}."
        )

    detected_as_working = (
        first_report["viability"] == 1
        and second_report["viability"] == 1
        and first_report["device_descriptor"] == second_report["device_descriptor"]
    )
    if detected_as_working != worked_in_practice:
        print(
            f"The CMA autodetection didn't correctly predict the behavior of the "
            f"syscall. It determined it would "
            f"{'succeed' if detected_as_working else 'fail'} whereas it actually "
            f"{'succeeded' if worked_in_practice else 'failed'}.",
            file=sys.stderr,
        )
        sys.exit(1)

    sys.exit(0)
