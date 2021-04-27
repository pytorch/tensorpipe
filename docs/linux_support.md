This document is intended for developers and advanced users. It’s the kind of document that risks going out of date very quickly, hence take it with a grain of salt.

In order to try to be as performant as possible, TensorPipe sometimes relies on new and advanced kernel features. This is causing issues to users who are building and/or running on old kernels. Hence, whenever we use such features, we should always “guard” them somehow, i.e., detect their availability at compile-time or (preferably) at runtime, and disable the backend or mark it non-viable. It is ok-ish for users with old kernels to not have access to all backends, as long as there’s always at least one backend they can use.

## Compile-time vs runtime, Linux vs glibc

Unfortunately, both the kernel version used for building and the one used for running affect whether we can use a feature. This means that the availability of a function or flag during build doesn’t mean it will be supported at runtime (this is especially true for the official builds of PyTorch). On the other hand, it also means that even if the runtime kernel supports a feature, we may not be able to use it because we didn’t have access to a system header when building (e.g., to get a flag). While sometimes we can “polyfill” this information, it’s not always doable.

An additional complication is added by the fact that we typically access syscalls through their glibc wrappers. First of all, this means we only get access to a syscall once glibc wraps it, which could happen years later. But it also means we link to a glibc symbol, and thus to a specific version of glibc’s shared object. With the kernel, using an unsupported feature results in a runtime error when first used, which we can catch; but with glibc we get a loader error due to missing symbols at startup, even if the user doesn’t use TensorPipe, even if we could “tolerate” these symbols’ absence. It is thus desirable at times to avoid the glibc wrappers.

## Common tricks for how to guard/polyfill

* Kernel flags are typically defined as preprocessor flags (i.e., `#define FOO`). This is stuff like `O_TMPFILE`, `MAP_SHARED_VALIDATE`, `PR_SET_PTRACER`, ... It’s easy to detect this in the code, with a `#ifdef FOO`, and since these flags are (usually?) constants, it’s also easy to define them ourselves. This “polyfill” allows us to build on an old kernel but still run on a new one.
* For a new-ish syscall, we probably don’t want to use the glibc wrapper, for the problems described above, and because it’s hard to detect its availability (the best option is a CMake check whose result we inject as a preprocessor flag). An alternative is to invoke it through the generic `syscall` syscall, using the `SYS_foo` flags. This could bring a few issues on its own (especially for 32bit systems) but for now it hasn’t come to bite us. This way we skip glibc entirely, and simply end up getting ENOSYS if the runtime kernel doesn’t support the syscall. Those `SYS_foo` flags are defined by glibc, but it seems glibc defines them automatically for all the syscalls it “finds” in the kernel, and not just for the syscalls that glibc supports. Unfortunately we cannot “polyfill” the `SYS_foo` flags if we don’t find them, because they have different values on different architectures.

## What do others do?

Since [Apr 2017](https://github.com/libuv/libuv/commit/4e6101388015c6d0879308d566f0a4b79edc0c13), libuv only supports Linux 2.6.32 (December 2009) and glibc 2.12 (May 2010). (This doesn’t mean earlier versions are necessarily broken, but that libuv reserves the right to break them). Libuv seems to be somewhat tied to the RedHat/CentOS releases, which are common and have a very long lifespan. It doesn’t make sense for us to support older versions than what libuv does, because if libuv decides to break them there’s nothing we can do.

PyTorch tries to support the [manylinux2014 platform](https://www.python.org/dev/peps/pep-0599/) (defined by Python for use in PyPI/pip), which allows up to glibc 2.17 (December 2012). However, it’s not clear if we’re there yet, and the previous version is `manylinux2010` which comes with glibc 2.12.

Hence a reasonable recommendation seems to be to draw the line at Linux 2.6.32 and glibc 2.12. However, people with older versions than those have already reported issues and asked for fixes, which we can probably consider on a case-by-case basis.

## Kernel features used by TensorPipe

### Linux 2.1.4 (October 1996)

* The `getresuid` and `getresgid` syscalls.

### Linux 2.3.16 (September 1999)

* The `/proc/sys/kernel/random/boot_id` file. See `random(4)`.

  No git hash as it predates the use of git by Linux

  https://github.com/torvalds/linux/blob/1da177e4c3f41524e886b7f1b8a0c1fc7321cac2/drivers/char/random.c#L1270-L1278

### Linux 2.3.20 (October 1999)

* The `PR_GET_DUMPABLE` flag for `prctl`.

  No git hash as it predates the use of git by Linux

  https://github.com/torvalds/linux/blob/1da177e4c3f41524e886b7f1b8a0c1fc7321cac2/include/linux/prctl.h#L10

### Linux 2.6.26 (July 2008)

* Version 3 of Linux capabilities. (Initial capability support, including the `capget` syscall, dates back to Linux 2.1.100, from May 1998). See `capget(2)`.

  https://github.com/torvalds/linux/commit/ca05a99a54db1db5bca72eccb5866d2a86f8517f

### Linux 3.2 (January 2012)

* Cross-Memory Attach (i.e., the `process_vm_readv` syscall). See `process_vm_readv(2)`.

  https://github.com/torvalds/linux/commit/fcf634098c00dd9cd247447368495f0b79be12d1

### Linux 3.4 (May 2012)

* The YAMA security module, and thus the `/proc/sys/kernel/yama/ptrace_scope` file. This includes the `PR_SET_PTRACER` and the `PR_SET_PTRACER_ANY` flags for `prctl`. See `ptrace(2)`.

  https://github.com/torvalds/linux/commit/2d514487faf188938a4ee4fb3464eeecfbdcf8eb
  https://github.com/torvalds/linux/commit/bf06189e4d14641c0148bea16e9dd24943862215

### Linux 3.8 (February 2013)

* The `/proc/[pid]/ns/[ns]` files. Although that directory, and the `net` file therein, were already present in 3.0, the `pid` and `user` ones only arrived in 3.8 and, more importantly, the ability to identify a namespace by the inode number of those files came in 3.8 (when they stopped being hardlinks and became symlinks). See `proc(5)` and `namespaces(7)` and others.

  https://github.com/torvalds/linux/commit/6b4e306aa3dc94a0545eb9279475b1ab6209a31f
  https://github.com/torvalds/linux/commit/13b6f57623bc485e116344fe91fbcb29f149242b
  https://github.com/torvalds/linux/commit/57e8391d327609cbf12d843259c968b9e5c1838f
  https://github.com/torvalds/linux/commit/cde1975bc242f3e1072bde623ef378e547b73f91
  https://github.com/torvalds/linux/commit/bf056bfa80596a5d14b26b17276a56a0dcb080e5
  https://github.com/torvalds/linux/commit/98f842e675f96ffac96e6c50315790912b2812be

### Linux 3.11 (September 2013)

* The `O_TMPFILE` flag for `open`. See `open(2)`.

  https://github.com/torvalds/linux/commit/60545d0d4610b02e55f65d141c95b18ccf855b6e

### Linux 3.17 (October 2014)

* The `memfd_create` syscall. See `memfd_create(2)`.

  https://github.com/torvalds/linux/commit/9183df25fe7b194563db3fec6dc3202a5855839c

### Linux 4.11 (April 2017)

* The `/sys/kernel/security/lsm` file in `securityfs` (a list of active Linux Security Modules).

  https://github.com/torvalds/linux/commit/d69dece5f5b6bc7a5e39d2b6136ddc69469331fe

### TODO

* All that sysfs PCIe stuff done by CUDA GDR (e.g., resolving GPUs and NICs to PCIe paths, getting the BAR1 size, ...), plus checking the nv_mem_peer module

## Glibc features required by TensorPipe

### Glibc 2.2.5 (January 2002)

* The `capget` function.

### Glibc 2.3.3 (December 2003)

* The `dlinfo` function. (All of `dlopen`, `dlclose`, `dlsym` and `dlerror` were present since at least glibc 2.0).

### Glibc 2.12 (May 2010)

* The `pthread_setname_np` function.
