package fetcher

import (
	"context"
	"math/big"

	"log/slog"

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
	jobs := make(chan *big.Int, 1000)
	go fs.transactionWorker(ctx, 1, jobs)
	// TODO add graceful shutdowns
	for {
		select {
		case err := <-sub.Err():
			slog.Error("subscription error", "err", err)
		case header := <-headers:
			slog.Info("fetched new block", "block_number", header.Number.String())
			jobs <- header.Number

		}
	}
	return nil
}

// TODO switch this to a custom internal library with worker pool, handling panics, retries, and exponential backoffs, spinning up variabling up and down based on load
func (fs *Fetcher) transactionWorker(ctx context.Context, id int, jobs <-chan *big.Int) {
	for headerNumber := range jobs {
		// TODO handle block reorgs, roll back? Keep in memory?
		block, err := fs.ethclient.BlockByNumber(ctx, headerNumber)
		if err != nil {
			slog.Error("failed to fetch block", "err", err)
			continue
		}

		// TODO, add any filters to noisy transactions, and send as batches with only useful info to kafka topic
		for _, tx := range block.Transactions() {
			slog.Info("transaction", "transaction_id", tx.Hash())
		}
	}
}
