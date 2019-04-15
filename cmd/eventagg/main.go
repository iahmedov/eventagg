package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/iahmedov/eventagg/config"
	"github.com/iahmedov/eventagg/pkg/aggregator"
	localmq "github.com/iahmedov/eventagg/pkg/mq/local"
	pfile "github.com/iahmedov/eventagg/pkg/persistence/file"
	"github.com/iahmedov/eventagg/pkg/server"

	// plugin registrations
	_ "github.com/iahmedov/eventagg/pkg/aggregator/lazy"
	_ "github.com/iahmedov/eventagg/pkg/aggregator/realtime"

	"github.com/go-kit/kit/log"
)

var version string

func init() {
	err := os.Setenv(config.Version, version)
	if err == nil {
		return
	}

	os.Setenv(config.Version, "version_not_provided")
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	var waitStopping sync.WaitGroup
	waitStopping.Add(1)
	go func() {
		Run(ctx)
		waitStopping.Done()
	}()

	select {
	case <-ctx.Done():
	case <-interrupt:
		cancelFunc()
	}
	waitStopping.Wait()
}

func runInGroup(ctx context.Context, g *errgroup.Group, fnc func(context.Context) error) {
	g.Go(func() error { return fnc(ctx) })
}

func Run(ctx context.Context) {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller, "version", version)

	configPath := flag.String("config", "", "config file path")
	flag.Parse()

	cfg, err := config.ReadFile(*configPath)
	if err != nil {
		logger.Log("event", "failed to fetch config", "path", configPath, "error", err)
		os.Exit(1)
	}

	filePersistence, err := pfile.New(pfile.Config{
		DataDir: cfg.Persistence.Dir,
		Count:   cfg.Persistence.Count,
	})
	if err != nil {
		logger.Log("event", "failed to setup persistence", "error", err)
		os.Exit(1)
	}
	queue := localmq.New()
	queue.Subscribe(filePersistence.Add)

	views := map[string]aggregator.View{}
	for _, aggCfg := range cfg.Aggregators {
		agg, err := aggregator.New(aggCfg.Name, aggCfg.Params)
		if err != nil {
			logger.Log("event", "failed to create aggregator", "error", err)
			os.Exit(1)
		}
		queue.Subscribe(agg.Add)

		if _, ok := views[aggCfg.Alias]; ok {
			logger.Log("event", "failed to register view",
				"error", fmt.Sprintf("already exist: %s", aggCfg.Alias))
			os.Exit(1)
		}
		views[aggCfg.Alias] = agg
		defer agg.Close() // will be closed at the end of Run() method
	}

	srv := server.New(server.Config{
		Port:        cfg.Server.Port,
		Queue:       queue,
		Aggregators: views,
	}, log.With(logger, "service", "api"))
	logger.Log("event", "service initialization finished, starting...")
	runner, groupCtx := errgroup.WithContext(ctx)
	runInGroup(groupCtx, runner, queue.Start)
	runInGroup(groupCtx, runner, srv.Run)
	if err := runner.Wait(); err != nil {
		logger.Log("event", "error", "cause", err)
	}

	return
}
