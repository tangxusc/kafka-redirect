package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"kafka-redirect/internal/config"
	"kafka-redirect/internal/service"
)

var runForwardFunc = service.RunForwardWithLogger
var runReplayFunc = service.RunReplayWithLogger

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("c", "", "path to config json file")
	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		return 2
	}

	jobs, err := config.LoadJobsAndValidate(*configPath)
	if err != nil {
		log.Printf("load config failed: %v", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	err = runJobs(ctx, jobs)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("service failed: %v", err)
		return 1
	}
	return 0
}

func runJobs(ctx context.Context, jobs []config.ValidatedJob) error {
	if len(jobs) == 0 {
		return errors.New("no validated jobs to run")
	}

	group, groupCtx := errgroup.WithContext(ctx)
	for _, item := range jobs {
		job := item
		group.Go(func() error {
			logger := buildJobLogger(job.Name, job.Mode)

			switch job.Mode {
			case config.ModeForward:
				return runForwardFunc(groupCtx, job.Config, logger)
			case config.ModeReplay:
				return runReplayFunc(groupCtx, job.Config, logger)
			default:
				return fmt.Errorf("unknown run mode: %s", job.Mode)
			}
		})
	}

	return group.Wait()
}

func buildJobLogger(name string, mode config.Mode) *log.Logger {
	return log.New(
		os.Stdout,
		fmt.Sprintf("[job=%s mode=%s] ", name, mode),
		log.LstdFlags,
	)
}
