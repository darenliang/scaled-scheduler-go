package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/alecthomas/kingpin/v2"
	"github.com/darenliang/scaled-scheduler-go/lib"
	"github.com/darenliang/scaled-scheduler-go/lib/logging"
	"github.com/darenliang/scaled-scheduler-go/lib/scheduler"
	"go.uber.org/zap"
)

var (
	address            = kingpin.Arg("address", "Scheduler address to connect to.").Required().String()
	workerTimeout      = kingpin.Flag("worker-timeout", "Discard worker when timeout is reached.").Default("1m").Duration()
	functionRetention  = kingpin.Flag("function-retention", "Discard function in scheduler when timeout is reached.").Default("1h").Duration()
	perWorkerQueueSize = kingpin.Flag("per-worker-queue-size", "Specify per worker queue size.").Default("1000").Int()
	debug              = kingpin.Flag("debug", "Print debug logs.").Default("false").Bool()
	version            = kingpin.CommandLine.Version(lib.Version)
)

func main() {
	kingpin.Parse()

	// init logger
	if *debug {
		logging.InitLogger(zap.DebugLevel)
	} else {
		logging.InitLogger(zap.InfoLevel)
	}

	// create context
	ctx, cancel := context.WithCancel(context.Background())

	// handle interrupt signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		cancel()
	}()

	s, err := scheduler.NewScheduler(ctx, *address, *perWorkerQueueSize, *workerTimeout, *functionRetention)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	s.Run()
}
