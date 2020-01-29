# tensorpipe

(insert subtitle)

(todo)

## Getting started

First clone the repository:

```shell
$ git clone --recursive https://github.com/pytorch/tensorpipe
```

Then, build as follows (using ninja instead of make):

``` shell
$ cd tensorpipe
$ mkdir build
$ cd build
$ cmake ../ -GNinja
$ ninja
```

You can find test executables in `build/tensorpipe/test`.
