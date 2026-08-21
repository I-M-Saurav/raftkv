package main

import (
	"log"

	"raftkv/internal/kvstore"
	"raftkv/internal/server"
)

func main() {
	store := kvstore.NewStore()
	httpServer := server.NewHTTPServer(store)

	if err := httpServer.Start(":8080"); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}