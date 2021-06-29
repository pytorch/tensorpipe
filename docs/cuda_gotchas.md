# CUDA gotchas

While implementing CUDA channels we hit some undocumented "quirks" which forced us to adapt our original designs. We collect them here for future reference (although this list may not be exhaustive). Please add more items whenever we learn new things "the hard way". We’re mostly interested in unexpected behaviors that could entail substantial design changes, although smaller technical pitfalls are welcome too.

## Most functions initialize a context on the current device

A lot of CUDA functions cause a CUDA context to be initialized for the "current" device (which is a thread-local variable managed by CUDA). This consumes on-device memory (plus it can cause deadlocks when combined with NCCL). By invoking CUDA functions without first explicitly setting the current device we risk accidentally initializing CUDA contexts on devices on which we weren’t supposed to (especially device 0, since it’s the "default"). In order to avoid this, a device guard should be used for *all* operations. They are very cheap, hence don’t be shy! At times it’s not clear which device should be used in such guard, for example during initialization, however we must only use devices that the user has explicitly provided, hence we may have to lazily delay initialization in those cases.

## Querying the device of a pointer can fail

By choice, TensorPipe doesn’t ask users to provide the device index when they pass in a CUDA pointer, for simplicity, since it would be redundant as the device index can be extracted from the pointer. This "extraction" is thus the only CUDA operation for which we can’t possibly set up a device guard. This has proven to be a problem because, due to a bug in CUDA, the extraction would fail if the current device had been *explicitly* set to an invalid (uninitialized) device. (A default "unset" current device would work). This occurred often, because if we used a device guard when the current device was unset, its destructor would explicitly reset the current device to 0. Our investigation seemed to show that an unset current device in the CUDA runtime corresponded to a null current context in the CUDA driver, whereas an invalid current device corresponded to an invalid non-null context. Thus our workaround was to use the driver API directly and first reset its current context to null (in a sense, use a "reverse" device guard, which temporarily "unsets" the current device).

## Releasing shared resources implicitly synchronizes

Some CUDA operations perform an implicit device synchronization: they block the CPU thread until the GPU "catches up", that is, it waits for *all* previously-launched kernels for that device (on any stream) to complete. Such functions also cause later kernels (enqueued by another concurrent thread) to delay their launch on the device until the blocking function returns (we’ve occasionally been calling this a "kernel fence"). This is bad because it would mean that an internal TensorPipe operation can interfere with the user’s scheduling of kernels and thus degrade GPU utilization. The [CUDA programming guide](https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html#implicit-synchronization) mentions such a behavior (in section 3.2.6.5.4), however we’ve found out that the list of circumstances where this occurs is incomplete and incorrect. As a rule of thumb, we’ve seen this behavior happen mainly when *releasing* a resource shared among kernels (e.g., device memory, pinned host memory, IPC memory handles), as if CUDA wanted to ensure there were no kernels using this resource anymore before freeing it. A mental model could be to imagine that kernels acquire a shared lock to it, while freeing it needs a unique lock. The only solution to this limitation is to allocate a pool of these resources at the beginning and reuse them.

## Creating IPC events deadlocks

Another CUDA bug we hit was that the creation of CUDA events with the interprocess flag would sometimes deadlock. [Here’s a (not so small) repro](https://gist.github.com/lw/f34836416e7674bbdda8b4925c2999f2). We couldn’t pin it down to a specific condition, or to a race with another call. NVIDIA confirmed the bug and supposedly fixed it in version 450 of the CUDA driver. Since we still need to support earlier versions, as a workaround we’re taking great care to create all our IPC events as early as possible (hoping to avoid whatever races) and reuse them.

## Memory won’t be freed if there’s open IPC handles to it

Imagine that process B has received and opened an IPC handle to some device memory allocated and owned by process A, and process A frees this memory without B first closing its handle to it. The CUDA doc described this as undefined behavior (hence we can’t complain), but in practice what we’ve observed is that the memory will *not* be freed, that is, it will not be reused for subsequent allocation requests, thus possibly causing OOMs. In a sense, it’s if as that memory were "leaked". This is displayed rather confusingly in `nvidia-smi`’s accounting: the memory appears as occupied in the device statistics, but no process appears to be responsible for it.

## Cannot open same IPC handle more than once

There’s a limitation in older versions of CUDA where, if process A allocates some memory, only *one* binding to it can be opened in process B using IPC handles. Attempting to re-open the same handle a second time will fail. Note that one cannot get multiple "different" handles for the same memory, as CUDA always returns the same one. In practice it means that the user could pass some memory for TensorPipe for which it has already manually created and shared a handle, thus it’s unsafe for TensorPipe to also get and open a handle. We can only safely do it for private memory that we’re managing ourselves. Also note that this limitation was lifted in CUDA 11.1.

## The pointer for an opened IPC handle could be "offset" wrt the source pointer

The CUDA doc on this is clear albeit cryptic: given a pointer, CUDA returns the IPC handle for its *allocation*. Hence if we allocate some memory at address p0 and ask for the IPC handle of address p1 = p0 + offset, we’ll get the IPC handle for p0! This means that when we open the handle we need to add back that offset. Luckily CUDA offers a function to query p0 given p1. Note that this situation happens a lot in PyTorch due to the caching allocator sometimes returning slices from larger blocks.

## Not all pairs of GPUs can access each other’s memory

Device to device (D2D) transfers are supported by CUDA only when peer-to-peer (P2P) capabilities exist between the two GPUs. This is handled transparently by CUDA, which will automatically select the most performant direct link. Concretely, it will use NVLink, but only if there’s a dedicated "cable" connecting those two devices. If the NVLink mesh is not a complete graph (as is often the case, e.g., hybrid-cube meshes (HCM) are very common), for the missing pairs CUDA will use PCIe transfers, but only if the two devices are attached to the same chipset/controller/host bridge. If there are multiple chipsets (which is also common, e.g., the DGX machines have two), then D2D transfers between some pairs of GPUs might just not be possible through CUDA! In principle this is easy enough to detect since CUDA offers a function for it (and `nvidia-smi topo` also displays it), however we can’t use it if the two devices aren’t both "visible" to the process (we’re referring to the `CUDA_VISIBLE_DEVICES` environment variable). For such cases the only option is to use the NVML library, which doesn’t honor that env var, but in turn adds the complexity of matching corresponding devices between CUDA and NVML (which is best done through their UUID). Moreover, additional complexity was required in TensorPipe to handle the case where some but not all pairs of GPUs between two processes supported P2P.

## Registering CUDA memory with IB is slow

This is kinda known, but it’s better to repeat it: the registration and deregistration of memory with InfiniBand is considered a "setup" step, and is very slow, and should thus be avoided as much as possible during the "hot" data path, for example using a staging area or by caching these registrations.

## Registering CUDA memory with IB requires an extra NVIDIA kernel module

When we pass a pointer to InfiniBand for registration, InfiniBand needs to understand that this virtual address points to CUDA device memory and not to some CPU memory. For that it needs to be aware of CUDA, and it does so through so-called "peer memory client", which NVIDIA provides (through a separate kernel module) and registers with InfiniBand, and which is queried by InfiniBand before "falling back" to assuming the pointer points to CPU memory. This peer memory client feature is only available in Mellanox’s InfiniBand distribution (called OFED, OpenFabrics Enterprise Distribution), and not in vanilla upstream InfiniBand. On the client side (our side) luckily nothing changes in the API.

## Registering CUDA memory with IB occupies the PCIe window

Each PCIe device has a handful of "memory windows" it exposes, through which the host or other devices can access and modify the device’s memory (both to issue commands and to send/retrieve data). These are called BARs (base address registers). In the case of NVIDIA GPUs the BAR that appears to map to the device’s main memory is BAR1. This is often sized much smaller than the memory itself (say, 256MB for a 16GB GPU), with the idea that it will just be used as a staging area. Also note that CUDA already reserves a few dozen MBs of that window. When registering CUDA device memory with InfiniBand, an additional mapping is created in that window (during the `ibv_reg_mr` call) and will thus fail if the window doesn’t have enough remaining space (e.g., if the buffer being registered is larger than the window). This means we can’t straightforwardly register the user-provided buffers. However, with the right combination of GPU and of CPU BIOS, the BAR1 can become as large as the GPU’s main memory itself, in which case this won’t be a problem anymore.

## Registering CUDA memory with IB doesn’t leak it

Contrary to IPC handles, freeing CUDA device memory while it’s still registered with InfiniBand does not appear to interfere with the deallocation, hence the memory will correctly become reusable.

## IB messages have a maximum size

Each send/recv operation over InfiniBand can only handle up to a certain amount of data, usually at least 1GB, and will fail for larger amounts. This limit can be queried on the device, and chunking must be used for larger sizes.

It appears that, at least on some NICs and with some drivers, there's also a "minimum size" of 32 bytes, with messages failing with odd errors for smaller sizes. It's still unclear whether it's a bug.

## GPUs need to be matched with the right IB NIC

On some machine types there may be multiple GPUs and multiple InfiniBand devices and they need to be carefully matched. Using the same IB NIC for all GPUs will introduce a bottleneck while leaving all other NICs unused. Matching them up "randomly" means that the data paths over PCIe of different GPU-NIC pairs might cross each other (thus, again, causing a bottleneck), might traverse the host, or otherwise interfere. These machines are usually set up so that each GPU has one NIC that it’s "naturally" closest to, for example they share the same PCIe switch, thus we need a logic to be able to detect and implement this.
