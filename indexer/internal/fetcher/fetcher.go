package fetcher

import (
	"context"
	workerpool "main/internal/utils/worker-pool"
	"math/big"

	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Fetcher struct {
	ethclient *ethclient.Client
}

func NewFetcher(ethClient *ethclient.Client) *Fetcher {
	return &Fetcher{ethclient: ethClient}
}

func (fs *Fetcher) SubscribeToLatestBlock(ctx context.Context) error {
	headers := make(chan *types.Header, 64)

	sub, err := fs.ethclient.SubscribeNewHead(ctx, headers)
	if err != nil {
		slog.Error("failed to subscribe to current blockchain head notification", "err", err)
		return err
	}
	defer sub.Unsubscribe()
	slog.Info("Listening to new blocks")
	pool := workerpool.NewWorkerPool[big.Int, [][]common.Hash](2, 10, 100, 100)
	pool.Start(ctx, 5)
	// go func() {
	// 	// temporary for testing
	// 	for tx := range pool.ResultChan {
	// 		fmt.Println(tx)
	// 	}
	// }()
	// TODO add graceful shutdowns
	for {
		select {
		case err := <-sub.Err():
			slog.Error("subscription error", "err", err)
		case header := <-headers:
			slog.Info("fetched new block", "block_number", header.Number.String())
			// TODO: should abstract this away into the library
			pool.JobChan <- workerpool.Job[big.Int, [][]common.Hash]{ID: 12, PayLoad: *header.Number, Execute: func(ctx context.Context, headerNumber big.Int) ([][]common.Hash, error) {
				return fs.transactionWorker(ctx, headerNumber)
			}}

		}
	}
	return nil
}

func (fs *Fetcher) transactionWorker(ctx context.Context, headerNumber big.Int) ([][]common.Hash, error) {
	block, err := fs.ethclient.BlockByNumber(ctx, &headerNumber)
	if err != nil {
		slog.Error("failed to fetch block", "err", err)
		return [][]common.Hash{}, nil
	}
	batchSize := 10
	var batches [][]common.Hash
	var batch []common.Hash
	for _, tx := range block.Transactions() {
		batch = append(batch, tx.Hash())

		if len(batch) == batchSize {
			batches = append(batches, batch)
			batch = nil
		}
	}

	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches, nil
}
