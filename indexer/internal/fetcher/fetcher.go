package fetcher

import (
	"github.com/ethereum/go-ethereum/ethclient"
)

type Fetcher struct {
	ethclient *ethclient.Client
}

func NewFetcher(ethClient *ethclient.Client) *Fetcher {
	return &Fetcher{ethclient: ethClient}
}
