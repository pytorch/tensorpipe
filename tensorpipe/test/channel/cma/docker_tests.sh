#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# We use a lot of trailing backslashes inside single-quoted string literals when
# we pass sub-scripts to sh -c, in order to wrap lines for long commands.
# Removing them would be incorrect, hence we just silence the linter warning.
# shellcheck disable=SC1004

set -eo pipefail


echo "Both endpoints in same vanilla container"
# This is not supposed to work, as Docker by default has a seccomp-bpf rule that
# blocks the process_vm_readv syscall.
# See https://jvns.ca/blog/2020/04/29/why-strace-doesnt-work-in-docker/
# and https://docs.docker.com/engine/security/seccomp/

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json & \
  probe1_pid=$!; \
  while [ ! -S /tmp/report/socket ]; do sleep 0.1; done; \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json & \
  probe2_pid=$!; \
  wait $probe1_pid; \
  wait $probe2_pid'

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 0


echo "Both endpoints in same container, seccomp-bpf disabled"
# This fixes the above problem, and makes it work.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json & \
  probe1_pid=$!; \
  while [ ! -S /tmp/report/socket ]; do sleep 0.1; done; \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json & \
  probe2_pid=$!; \
  wait $probe1_pid; \
  wait $probe2_pid'

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "Both endpoints in same container, capability SYS_PTRACE added"
# This should not really matter, but Docker adds a "side effect" to this which
# also re-enables process_vm_readv in seccomp-bpf.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --cap-add SYS_PTRACE \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json & \
  probe1_pid=$!; \
  while [ ! -S /tmp/report/socket ]; do sleep 0.1; done; \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json & \
  probe2_pid=$!; \
  wait $probe1_pid; \
  wait $probe2_pid'

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "Both endpoints in same container, privileged"
# This should not really matter, but Docker adds a "side effect" to this which
# also re-enables process_vm_readv in seccomp-bpf.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --privileged \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json & \
  probe1_pid=$!; \
  while [ ! -S /tmp/report/socket ]; do sleep 0.1; done; \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json & \
  probe2_pid=$!; \
  wait $probe1_pid; \
  wait $probe2_pid'

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "Both endpoints in same container, stronger YAMA limits"
# CMA is able to work under YAMA when the latter is set to levels 0 or 1, as
# in the first case YAMA adds no extra limit and in the second case CMA will
# configure YAMA so that it allows the process to be ptraced by any other one.
# However CMA can't handle YAMA at level 2 or higher.
# We keep disabling seccomp-bpf as otherwise this would be shadowed.

sudo sh -c 'echo 2 > /proc/sys/kernel/yama/ptrace_scope'

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json & \
  probe1_pid=$!; \
  while [ ! -S /tmp/report/socket ]; do sleep 0.1; done; \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json & \
  probe2_pid=$!; \
  wait $probe1_pid; \
  wait $probe2_pid'

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 0

sudo sh -c 'echo 1 > /proc/sys/kernel/yama/ptrace_scope'


# TODO
# echo "Both endpoints in same container, different users/groups"


# TODO
# echo "Both endpoints in same container, same users/groups but different effective user/group"


echo "Each endpoint in own container, with separate namespace"
# This isn't supposed to work, as each container gets its own user and PID
# namespace, but CMA needs them to match. We disable seccomp-bpf to give this
# test a fighting chance.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json' &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 0


# Docker allows a container to reuse another one's PID namespace, but doesn't
# allow the same for user namespaces.


echo "Each endpoint in own container, reusing host namespaces"
# This should fix the issues of the above.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  --pid host \
  --userns host \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  --pid host \
  --userns host \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json' &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "Each endpoint in own container, privileged, sharing PID namespace"
# This should also help.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --cidfile "$TEMPDIR/probe1_container_id" \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --privileged \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --pid "container:$(cat "$TEMPDIR/probe1_container_id")" \
  --privileged \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  1 /tmp/report/socket \
  > /tmp/report/probe2_report.json' &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "One endpoint on host, other in container, with own namespace"
# This isn't supposed to work, as each container gets its own user and PID
# namespace, but CMA needs them to match. We disable seccomp-bpf to give this
# test a fighting chance. And also AppArmor, as it starts mattering here,
# because Docker sets its own profile (docker-default) which is different than
# the host's one (unconfined).

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  --security-opt apparmor=unconfined \
  --user "$(id -u):$(id -g)" \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
sudo chmod ugo+rwx "$TEMPDIR"/socket
TP_VERBOSE_LOGGING=5 \
  "$(pwd)/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe" \
  1 "$TEMPDIR/socket" \
  > "$TEMPDIR/probe2_report.json" &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 0


echo "One endpoint on host, other in container, reusing host namespace"
# This should fix the issues of the above.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  --security-opt apparmor=unconfined \
  --pid host \
  --userns host \
  --user "$(id -u):$(id -g)" \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
sudo chmod ugo+rwx "$TEMPDIR"/socket
TP_VERBOSE_LOGGING=5 \
  "$(pwd)/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe" \
  1 "$TEMPDIR/socket" \
  > "$TEMPDIR/probe2_report.json" &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "One endpoint on host, other in container, privileged"
# This should also help.

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

docker run \
  --volume "$TEMPDIR:/tmp/report" \
  --volume "$(pwd)/build:/tmp/build" \
  --security-opt seccomp=unconfined \
  --security-opt apparmor=unconfined \
  --pid host \
  --user "$(id -u):$(id -g)" \
  --privileged \
  cimg/base:2020.01 \
  sh -c ' \
  TP_VERBOSE_LOGGING=5 \
  /tmp/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe \
  0 /tmp/report/socket \
  > /tmp/report/probe1_report.json' &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
sudo chmod ugo+rwx "$TEMPDIR"/socket
TP_VERBOSE_LOGGING=5 \
  "$(pwd)/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe" \
  1 "$TEMPDIR/socket" \
  > "$TEMPDIR/probe2_report.json" &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1


echo "Both endpoints on host"
# Should be a no-brainer?

TEMPDIR=$(mktemp --directory)
chmod ugo+rwx "$TEMPDIR"
echo "Using $TEMPDIR for staging data"

TP_VERBOSE_LOGGING=5 \
  "$(pwd)/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe" \
  0 "$TEMPDIR/socket" \
  > "$TEMPDIR/probe1_report.json" &
probe1_pid=$!
while [ ! -S "$TEMPDIR/socket" ]; do sleep 0.1; done
TP_VERBOSE_LOGGING=5 \
  "$(pwd)/build/tensorpipe/test/channel/cma/tensorpipe_channel_cma_probe" \
  1 "$TEMPDIR/socket" \
  > "$TEMPDIR/probe2_report.json" &
probe2_pid=$!
wait $probe1_pid
wait $probe2_pid

python3 \
  "$(pwd)/tensorpipe/test/channel/cma/probe_report_checker.py" \
  "$TEMPDIR/probe1_report.json" "$TEMPDIR/probe2_report.json" 1
