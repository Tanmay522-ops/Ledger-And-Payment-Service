package db

import (
	"context"
	"database/sql"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Open() (*sql.DB, error) {
	url := Getenv("LEDGER_DB_URL",
		"postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable")
	d, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(50) // enough for 100 goroutines to overlap
	return d, nil
}

// Reset wipes everything and re-applies the migration files in order.
// TEST USE ONLY.
func Reset(ctx context.Context, d *sql.DB, files ...string) error {
	if _, err := d.ExecContext(ctx,
		`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		return err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := d.ExecContext(ctx, string(b)); err != nil {
			return err
		}
	}
	return nil
}