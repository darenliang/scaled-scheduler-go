# Scaled Scheduler in Go

This scheduler implements the protocol used in [scaled](https://github.com/yzard/scaled), making it a drop-in
replacement for scaled's Python scheduler.

> :warning: The scaled scheduler protocol is not fixed and is subject
> to change. This scheduler doesn't fully implement the protocol and may have bugs.

## Dependencies

- [libsodium](https://github.com/jedisct1/libsodium)
- [libzmq](https://github.com/zeromq/libzmq)
- [czmq](https://github.com/zeromq/czmq)

## Building

Go 1.19+ is supported and cgo must be enabled.

```bash
go build
```

## Usage

```bash
scaled-scheduler-go tcp://0.0.0.0:8786
```
