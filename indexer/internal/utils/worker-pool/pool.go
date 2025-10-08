package workerpool

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"
)

type BackoffFunc func(retry int) time.Duration

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
	wg         sync.WaitGroup
}

// TODO move this to config?
func NewWorkerPool[P any, R any](maxRetries int, jobBufferSize int, resultBufferSize int, deadLetterBufferSize int) *WorkerPool[P, R] {
	return &WorkerPool[P, R]{
		MaxRetries: maxRetries,
		JobChan:    make(chan Job[P, R], jobBufferSize),
		ResultChan: make(chan Result[R], resultBufferSize),
		DLQ:        make(chan DeadLetter[P], deadLetterBufferSize),
		wg:         sync.WaitGroup{},
	}
}

func (wp *WorkerPool[P, R]) Start(ctx context.Context, workerCount int) {
	backoffWJitter := backoffWithJitter(time.Millisecond*10, time.Duration(time.Second*2))
	for i := range workerCount {
		wp.wg.Add(1)
		go wp.worker(ctx, i, backoffWJitter)
	}
}

// TODO add graceful shut down, waitgroup check, etc.
func (wp *WorkerPool[P, R]) worker(ctx context.Context, workerID int, backoffWJitter BackoffFunc) {
	defer wp.wg.Done()
	for job := range wp.JobChan {
		attempts := 0
		for {
			fmt.Printf("worker %d doing job\n", workerID)
			result, err := func() (res R, err error) {
				defer func() {
					if r := recover(); r != nil {
						slog.Warn("panic in worker", "worker_id", workerID)
					}
				}()
				return job.Execute(ctx, job.PayLoad)
			}()

			if err == nil {
				wp.ResultChan <- Result[R]{JobID: workerID, Value: result}
				fmt.Printf("worker %d done job\n", workerID)

				break
			}

			if attempts >= wp.MaxRetries {
				wp.DLQ <- DeadLetter[P]{JobID: workerID, PayLoad: job.PayLoad, Error: err}
				break
			}
			delay := backoffWJitter(attempts)
			time.Sleep(delay)
		}
	}
}

func (wp *WorkerPool[P, R]) Stop(ctx context.Context) {
	close(wp.JobChan)
	wp.wg.Wait()
	close(wp.DLQ)
	close(wp.ResultChan)
}

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
func backoffWithJitter(baseTime time.Duration, maxDelay time.Duration) BackoffFunc {
	return func(retry int) time.Duration {
		backoff := time.Duration(math.Min(float64(baseTime)*math.Pow(2, float64(retry)), float64(maxDelay)))
		// add jitter with the backoff time
		//TODO create a wrapper for the random generator to deterministically test them
		return time.Duration(rand.Float64() * float64(backoff))
	}
}
