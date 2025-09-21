package pipeline

import (
	"context"
	"log/slog"
)

type Fetcher interface {
	SubscribeToLatestBlock(ctx context.Context) error
}

type Pipeline struct {
	fetcher Fetcher
}

func NewPipeline(fetcher Fetcher) *Pipeline {
	return &Pipeline{fetcher: fetcher}
}

func (pipeline *Pipeline) Run(ctx context.Context) error {
	err := pipeline.fetcher.SubscribeToLatestBlock(ctx)
	if err != nil {
		slog.Error("failed to subscribe to latest block", "err", err)
	} 
	return nil
}
