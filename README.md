# Scaled Scheduler in Go

This scheduler implements the protocol used in [scaled](https://github.com/yzard/scaled), making it a drop-in
replacement for scaled's Python scheduler.

> :warning: This scheduler doesn't fully implement the protocol and may have bugs.
> The scaled scheduler protocol is subject to change so there may be compatibility issues.

## Dependencies

- [libzmq](https://github.com/zeromq/libzmq)

## Building

Go 1.19+ is supported and cgo must be enabled.

```bash
go build
```

## Usage

```bash
scaled-scheduler-go tcp://0.0.0.0:8786
```
