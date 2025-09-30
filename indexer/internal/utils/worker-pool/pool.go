package workerpool

import (
	"context"
	"fmt"
)

type Job[P any, R any] struct {
	ID      int
	PayLoad P
	Execute func(ctx context.Context, payload P) (R, error)
}

type Result[R any] struct {
	JobID int
	Value R
}
type DeadLetter[P any] struct {
	JobID   int
	PayLoad P
	Error   error
}

type WorkerPool[P any, R any] struct {
	MaxRetries int
	JobChan    chan Job[P, R]
	ResultChan chan Result[R]
	DLQ        chan DeadLetter[P]
}

// TODO move this to config?
func NewWorkerPool[P any, R any](maxRetries int, jobBufferSize int, resultBufferSize int, deadLetterBufferSize int) *WorkerPool[P, R] {
	return &WorkerPool[P, R]{
		MaxRetries: maxRetries,
		JobChan:    make(chan Job[P, R], jobBufferSize),
		ResultChan: make(chan Result[R], resultBufferSize),
		DLQ:        make(chan DeadLetter[P], deadLetterBufferSize),
		//TODO need a waitgroup for this
	}
}

func (wp *WorkerPool[P, R]) Start(ctx context.Context, workerCount int) {
	for i := range workerCount {
		go wp.worker(ctx, i)
	}
}

// TODO add graceful shit down, waitgroup check, etc.
func (wp *WorkerPool[P, R]) worker(ctx context.Context, workerID int) {
	for job := range wp.JobChan {
		attempts := 0
		for {
			fmt.Printf("worker %d doing job\n", workerID)
			result, err := job.Execute(ctx, job.PayLoad)
			if err == nil {
				wp.ResultChan <- Result[R]{JobID: workerID, Value: result}
				fmt.Printf("worker %d done job\n", workerID)

				break
			}

			if attempts >= wp.MaxRetries {
				wp.DLQ <- DeadLetter[P]{JobID: workerID, PayLoad: job.PayLoad, Error: err}
				break
			}

		}
	}
}
