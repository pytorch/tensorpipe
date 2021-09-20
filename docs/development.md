# Development

TensorPipe uses CMake for its build system.

## Dependencies

To build TensorPipe, you need:

* C++14 compatible compiler (GCC >= 5.5 or Clang >= 6)

## Clone the repository

Example:

``` shell
git clone --recursive https://github.com/pytorch/tensorpipe
```

If you have updated an already cloned repository, make sure that the
submodules are up to date:

``` shell
git submodule sync
git submodule update --init
```

It is imperative to check out the submodules before running CMake.

Find the list of submodules and a description of what they're used for
on [this page][third_party].

[third_party]: https://github.com/pytorch/tensorpipe/tree/main/third_party

## Using CMake

Example:

``` shell
mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread
make
```

You can specify CMake variables by passing them as arguments to the `cmake` command.

Useful CMake variables:

* `CMAKE_C_COMPILER` -- Define which C compiler to use.
* `CMAKE_CXX_COMPILER` -- Define which C++ compiler to use.
* `CMAKE_C_FLAGS` -- Additional flags for the C compiler.
* `CMAKE_CXX_FLAGS` -- Additional flags for the C++ compiler.
* `CMAKE_BUILD_TYPE` -- For example: `release`, `debug`.

Useful TensorPipe specific variables:

* `SANITIZE` -- configure the sanitizer to use (if any); for
  example: `address` or `thread`, to run with `asan` or `tsan`,
  respectively.

## Ninja

To make CMake output something other than the default `Makefile`, see
[`cmake-generators(7)`][cmake-generators]. We like to use the
[Ninja][ninja] generator because it works well for incremental builds.
On the command line, specify `-GNinja` to use it.

[cmake-generators]: https://cmake.org/cmake/help/v3.4/manual/cmake-generators.7.html
[ninja]: https://en.wikipedia.org/wiki/Ninja_(build_system)
