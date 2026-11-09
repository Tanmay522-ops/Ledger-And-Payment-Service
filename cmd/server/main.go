package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"ledger/internal/api"
	"ledger/internal/db"
	"ledger/internal/ledger"
)

func main() {
	d, err := db.Open()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := d.PingContext(ctx); err != nil {
		log.Fatal("database not reachable: ", err)
	}
	ext, err := ledger.EnsureExternal(ctx, d)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("External account id = %d", ext)

	srv := &http.Server{
		Addr:              db.Getenv("LEDGER_ADDR", ":8080"),
		Handler:           (&api.Server{DB: d}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}