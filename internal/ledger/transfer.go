package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"ledger/internal/db"
)

// afterDebitHook is a TEST-ONLY failure-injection point. It is nil in real runs.
// Tests set it to simulate a failure between the debit and the credit.
var afterDebitHook func() error

type savedResponse struct {
	TransferID int64  `json:"transfer_id,omitempty"`
	Error      string `json:"error,omitempty"`
}

func fingerprint(req TransferRequest) string {
	return fmt.Sprintf("%d-%d-%d", req.From, req.To, req.Amount)
}

func validate(req TransferRequest) error {
	switch {
	case req.Key == "":
		return ErrKeyRequired
	case req.Amount <= 0:
		return ErrInvalidAmount
	case req.Amount > MaxAmount:
		return ErrAmountTooLarge
	case req.From == req.To:
		return ErrSameAccount
	}
	return nil
}

func lockAccount(ctx context.Context, tx *sql.Tx, id int64) (kind string, bal int64, err error) {
	err = tx.QueryRowContext(ctx,
		`SELECT kind, balance FROM accounts WHERE id = $1 FOR UPDATE`, id).
		Scan(&kind, &bal)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, ErrAccountNotFound
	}
	return kind, bal, err
}

// Transfer is the SAFE version: one transaction, row locks (lower ID first),
// and the idempotency key claimed inside the same transaction.
func Transfer(ctx context.Context, d *sql.DB, req TransferRequest) (*TransferResult, error) {
	res, err := transfer(ctx, d, req)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" { // lock_not_available
		return nil, ErrInProgress
	}
	return res, err
}

func transfer(ctx context.Context, d *sql.DB, req TransferRequest) (*TransferResult, error) {
	if err := validate(req); err != nil {
		return nil, err // malformed: nothing attempted, nothing saved
	}
	fp := fingerprint(req)

	tx, err := d.BeginTx(ctx, nil) // default isolation: READ COMMITTED
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // does nothing after a successful Commit

	// Give up waiting on a lock after this long (default 5s).
	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('lock_timeout', $1, true)`,
		db.Getenv("LEDGER_LOCK_TIMEOUT", "5s")); err != nil {
		return nil, err
	}

	// An expired key counts as new: remove its old row first.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM idempotency_keys WHERE key = $1 AND expires_at < now()`,
		req.Key); err != nil {
		return nil, err
	}

	// Claim the key. A concurrent duplicate WAITS here until we commit or roll back.
	claim, err := tx.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, fingerprint, response, expires_at)
		 VALUES ($1, $2, '{}'::jsonb, now() + interval '24 hours')
		 ON CONFLICT (key) DO NOTHING`, req.Key, fp)
	if err != nil {
		return nil, err
	}
	if n, _ := claim.RowsAffected(); n == 0 {
		return replay(ctx, tx, req.Key, fp) // key already exists and is committed
	}

	// Lock both accounts, LOWER ID FIRST, whatever the direction.
	lo, hi := req.From, req.To
	if lo > hi {
		lo, hi = hi, lo
	}
	kinds := map[int64]string{}
	bals := map[int64]int64{}
	for _, id := range []int64{lo, hi} {
		k, b, err := lockAccount(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		kinds[id], bals[id] = k, b
	}

	if kinds[req.From] == "system" && kinds[req.To] == "system" {
		return nil, ErrNeedUserAccount
	}

	// The check now uses a locked, fresh balance. No one can change it under us.
	if kinds[req.From] == "user" && bals[req.From] < req.Amount {
		body, _ := json.Marshal(savedResponse{Error: "insufficient_funds"})
		if _, err := tx.ExecContext(ctx,
			`UPDATE idempotency_keys SET response = $2::jsonb WHERE key = $1`,
			req.Key, string(body)); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil { // COMMIT, not rollback: remember the decline
			return nil, err
		}
		return nil, ErrInsufficientFunds
	}

	var tid int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO transfers (from_account, to_account, amount)
		 VALUES ($1,$2,$3) RETURNING id`,
		req.From, req.To, req.Amount).Scan(&tid); err != nil {
		return nil, err
	}
		if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET balance = balance - $1 WHERE id = $2`, req.Amount, req.From); err != nil {
		return nil, err
	}
	if afterDebitHook != nil { // test-only failure point: debit done, credit not yet
		if err := afterDebitHook(); err != nil {
			return nil, err // deferred Rollback undoes the debit, transfer row, everything
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET balance = balance + $1 WHERE id = $2`, req.Amount, req.To); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger_entries (transfer_id, account_id, amount)
		 VALUES ($1,$2,$3),($1,$4,$5)`,
		tid, req.From, -req.Amount, req.To, req.Amount); err != nil {
		return nil, err
	}

	body, _ := json.Marshal(savedResponse{TransferID: tid})
	if _, err := tx.ExecContext(ctx,
		`UPDATE idempotency_keys SET response = $2::jsonb, transfer_id = $3 WHERE key = $1`,
		req.Key, string(body), tid); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &TransferResult{TransferID: tid}, nil
}

// replay handles "this key already exists and is committed".
func replay(ctx context.Context, tx *sql.Tx, key, fp string) (*TransferResult, error) {
	rec, err := findKey(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, errors.New("key disappeared during replay, retry the request")
	}
	if rec.Fingerprint != fp { // re-read AFTER any wait, then compare
		return nil, ErrKeyConflict
	}
	var saved savedResponse
	if err := json.Unmarshal(rec.Response, &saved); err != nil {
		return nil, err
	}
	if saved.Error == "insufficient_funds" {
		return nil, ErrInsufficientFunds
	}
	return &TransferResult{TransferID: saved.TransferID, Replayed: true}, nil
}