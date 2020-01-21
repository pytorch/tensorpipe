# Development

TensorPipe uses CMake for its build system.

## Using CMake

Example:

``` shell
mkdir build
cmake ../ -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=thread
make
```

You can specify CMake variables by passing them as arguments to the `cmake` command.

Useful CMake variables:

* `CMAKE_BUILD_TYPE` -- for example: `release`, `debug`.

Useful TensorPipe specific variables:

* `SANITIZE` -- configure the sanitizer to use (if any); for
  example: `address` or `thread`, to run with `asan` or `tsan`,
  respectively.

### sccache

If you have [`sccache`][sccache] installed and available in your
`PATH`, the build system will automatically find and use it.

If you're not sure if `sscache` is being used for your build, check
out its cache statistics before and after a build by running:

``` shell
$ sccache -s
```

[sccache]: https://github.com/mozilla/sccache

### Colorized output with sccache

By default, `sccache` will strip color information from the compiler
output. To make it pass through the compilers colorized output, you
have to install `sccache` version 0.2.13 or higher (as of 2020-01-21
this version is not yet available and you have to build `sscache` from
source -- this features was added in [mozilla/sccache#589][pr-589]).

[pr-589]: https://github.com/mozilla/sccache/pull/589

After getting the right version, configure the compiler to always
produce colorized output by passing the CMake variable
`-DCMAKE_CXX_FLAGS=-fdiagnostics-color`

## Ninja

To make CMake output something other than the default `Makefile`, see
[`cmake-generators(7)`][cmake-generators]. We like to use the
[Ninja][ninja] generator because it works well for incremental builds.
On the command line, specify `-GNinja` to use it.

[cmake-generators]: https://cmake.org/cmake/help/v3.4/manual/cmake-generators.7.html
[ninja]: https://en.wikipedia.org/wiki/Ninja_(build_system)
