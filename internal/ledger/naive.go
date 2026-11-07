package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// NaiveDelay widens the gap between "check" and "write" so the race shows up
// reliably. Tests set it. It is 0 in normal use.
var NaiveDelay time.Duration

type querier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

// findKey is the ONLY place idempotency keys are looked up.
func findKey(ctx context.Context, q querier, key string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx,
		`SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n)
	return n > 0, err
}

// NaiveTransfer is DELIBERATELY UNSAFE. Do not fix it.
func NaiveTransfer(ctx context.Context, d *sql.DB, req TransferRequest) (*TransferResult, error) {
	seen, err := findKey(ctx, d, req.Key)
	if err != nil {
		return nil, err
	}
	if seen {
		return &TransferResult{Replayed: true}, nil
	}

	var fromBal int64
	var fromKind string
	if err := d.QueryRowContext(ctx,
		`SELECT balance, kind FROM accounts WHERE id = $1`, req.From).
		Scan(&fromBal, &fromKind); err != nil {
		return nil, err
	}

	// MISTAKE: check in Go, using a read that may already be stale.
	if fromKind == "user" && fromBal < req.Amount {
		return nil, ErrInsufficientFunds
	}

	time.Sleep(NaiveDelay) // another request can sneak in here

	var tid int64
	if err := d.QueryRowContext(ctx,
		`INSERT INTO transfers (from_account, to_account, amount)
		 VALUES ($1,$2,$3) RETURNING id`,
		req.From, req.To, req.Amount).Scan(&tid); err != nil {
		return nil, err
	}
	// MISTAKE: no transaction. Each statement commits on its own.
	if _, err := d.ExecContext(ctx,
		`UPDATE accounts SET balance = balance - $1 WHERE id = $2`, req.Amount, req.From); err != nil {
		return nil, err
	}
	if _, err := d.ExecContext(ctx,
		`UPDATE accounts SET balance = balance + $1 WHERE id = $2`, req.Amount, req.To); err != nil {
		return nil, err
	}
	if _, err := d.ExecContext(ctx,
		`INSERT INTO ledger_entries (transfer_id, account_id, amount)
		 VALUES ($1,$2,$3),($1,$4,$5)`,
		tid, req.From, -req.Amount, req.To, req.Amount); err != nil {
		return nil, err
	}
	// MISTAKE: key recorded only at the very end.
	if _, err := d.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, fingerprint, response, transfer_id, expires_at)
		 VALUES ($1,$2,'{}'::jsonb,$3, now() + interval '24 hours')`,
		req.Key, fmt.Sprintf("%d-%d-%d", req.From, req.To, req.Amount), tid); err != nil {
		return nil, err
	}
	return &TransferResult{TransferID: tid}, nil
}