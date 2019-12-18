# shared memory

> This code was written by David Carrillo Cisneros (davidca@fb.com).
> Modified to work with C++14 by Pieter Noordhuis (pietern@fb.com)

A wrapper around Linux's shared-memory mechanisms to create, load,
link and unlink shared memory regions.

Current version is hardcode to work create files and subfolders
inside /dev/shm/tensorpipe/ .

Permissions for shared memory follow Linux file system permissions.
E.g. an authorized users can inspect memory content by dumping the
content of the shared memory file content with:

```
$ xxd /dev/shm/tensorpipe/<your file>
```

## Design choices

1. Decided against using Boost's shared memory to leverage Linux-only such as:
  a. directory structure inside of shared memory,
  b. huge TLB pages
  c. fine-tuned memory write-only/read-only page modes
  (e.g. allow segments as write only, reducing cache contention).

2. Support load at any memory address. To simplify user experience,
do not require segments to always be loaded in same memory address.

3. Do not provide mechanisms to find objects within each segment (i.e. no
mechanism similar to Boost's named objects in shared memory). This choice
greatly simplifies implementation and it is assumed that users of this
library will often find it better to roll-out their own mechanism to find
objects within the shared memory segment.
