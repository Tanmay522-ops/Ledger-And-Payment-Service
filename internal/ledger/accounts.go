package ledger

import (
	"context"
	"database/sql"
	"errors"
)

type Entry struct {
	Seq        int64 `json:"seq"`
	TransferID int64 `json:"transfer_id"`
	Amount     int64 `json:"amount"`
}

func CreateAccount(ctx context.Context, d *sql.DB, kind string) (int64, error) {
	var id int64
	err := d.QueryRowContext(ctx,
		`INSERT INTO accounts (kind) VALUES ($1) RETURNING id`, kind).Scan(&id)
	return id, err
}

func GetBalance(ctx context.Context, d *sql.DB, id int64) (int64, error) {
	var b int64
	err := d.QueryRowContext(ctx, `SELECT balance FROM accounts WHERE id = $1`, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrAccountNotFound
	}
	return b, err
}

// ListEntries returns entries in seq order, starting after afterSeq.
func ListEntries(ctx context.Context, d *sql.DB, id, afterSeq int64, limit int) ([]Entry, error) {
	if _, err := GetBalance(ctx, d, id); err != nil {
		return nil, err
	}
	rows, err := d.QueryContext(ctx,
		`SELECT seq, transfer_id, amount FROM ledger_entries
		 WHERE account_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, id, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Entry{}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Seq, &e.TransferID, &e.Amount); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EnsureExternal creates the External system account once and returns its ID.
// Fixed ID is not guaranteed, so it is found by a name-free rule: the first system account.
func EnsureExternal(ctx context.Context, d *sql.DB) (int64, error) {
	var id int64
	err := d.QueryRowContext(ctx,
		`SELECT id FROM accounts WHERE kind = 'system' ORDER BY id LIMIT 1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return CreateAccount(ctx, d, "system")
	}
	return id, err
}