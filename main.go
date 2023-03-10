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
	maxRequestWorkers  = kingpin.Flag("max-request-workers", "Specify max number of workers to handle request. A non-positive value signifies no limit.").Default("-1").Int()
	logLevel           = kingpin.Flag("log-level", "Specify log level.").Default("info").Enum("debug", "info", "warn", "error")
	_                  = kingpin.CommandLine.Version(lib.Version)
)

func main() {
	kingpin.Parse()

	// init logger
	switch *logLevel {
	case "debug":
		logging.InitLogger(zap.DebugLevel)
	case "info":
		logging.InitLogger(zap.InfoLevel)
	case "warn":
		logging.InitLogger(zap.WarnLevel)
	case "error":
		logging.InitLogger(zap.ErrorLevel)
	}

	// create context
	ctx, cancel := context.WithCancel(context.Background())

	// handle interrupt signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		logging.Logger.Info("received interrupt signal, shutting down scheduler")
		cancel()
	}()

	s, err := scheduler.NewScheduler(ctx, *address, *workerTimeout, *functionRetention, *perWorkerQueueSize, *maxRequestWorkers)
	if err != nil {
		logging.Logger.Fatal(err)
	}
	s.Run()
}
