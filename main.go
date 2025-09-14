package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ethNodeURL := os.Getenv("ETH_NODE_URL")
	ethNodeClient, err := ethclient.Dial(ethNodeURL)
	if err != nil {
		slog.Error("failed connect to eth node", "err", err)
	}
	slog.Debug("connected to eth node successfully")

	block, err := ethNodeClient.BlockNumber(context.Background())
	if err != nil {
		slog.Error("error fetching current eth block number", "err", err)
	}
	slog.Info("succesfully retrieved latest block number", "block_number", block)
}
