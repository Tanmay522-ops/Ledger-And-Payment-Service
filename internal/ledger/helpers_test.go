package ledger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"testing"

	"ledger/internal/db"
)

func newKey() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// setupNaive: clean DB, schema WITHOUT the balance CHECK.
func setupNaive(t *testing.T) *sql.DB {
	t.Helper()
	d, err := db.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	err = db.Reset(context.Background(), d,
		"../../migrations/001_init.sql",
		"../../migrations/naive_drop_check.sql")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func newAccount(t *testing.T, d *sql.DB, kind string) int64 {
	t.Helper()
	var id int64
	if err := d.QueryRow(
		`INSERT INTO accounts (kind) VALUES ($1) RETURNING id`, kind).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// newTransferRequest: the one place tests build requests.
func newTransferRequest(from, to, amount int64) TransferRequest {
	return TransferRequest{Key: newKey(), From: from, To: to, Amount: amount}
}

func balance(t *testing.T, d *sql.DB, id int64) int64 {
	t.Helper()
	var b int64
	if err := d.QueryRow(`SELECT balance FROM accounts WHERE id=$1`, id).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

// fund deposits via External -> user, the same way real deposits work.
func fund(t *testing.T, d *sql.DB, external, user, amount int64) {
	t.Helper()
	if _, err := NaiveTransfer(context.Background(), d,
		newTransferRequest(external, user, amount)); err != nil {
		t.Fatal(err)
	}
}