# Scaled Scheduler in Go

This scheduler implements the protocol used in [scaled](https://github.com/yzard/scaled), making it a drop-in
replacement for scaled's Python scheduler.

> :warning: The scaled scheduler protocol is subject to change so there may be compatibility issues.

## Building

### Using libzmq as a shared library

Go 1.19+ is supported and cgo must be enabled.

```bash
go build
```

### Standalone binary

Docker is the recommended way to build a standalone binary.

```bash
./build.sh
```

## Usage

To get the list of command line options:

```
scaled-scheduler-go --help
```

If you are ok with using default options, you can simply run the scheduler as follows:

```bash
scaled-scheduler-go tcp://0.0.0.0:8786
```

## Todo

* Create an interface for task allocators so multiple implementations can be used interchangeably.
    * Current task allocator uses a priority queue (heap) with each allocation costing O(log n).
* Implement a memory limiter and spill in-memory tasks to a disk-backed cache.
    * An embedded database like [BadgerDB](https://github.com/dgraph-io/badger) could be used for this.
