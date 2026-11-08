package ledger

import (
	"context"
	"database/sql"
	"errors"
)

type querier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

type keyRecord struct {
	Fingerprint string
	Response    []byte
}

// findKey is the ONLY place idempotency keys are looked up.
// It returns nil, nil when the key does not exist.
func findKey(ctx context.Context, q querier, key string) (*keyRecord, error) {
	var r keyRecord
	err := q.QueryRowContext(ctx,
		`SELECT fingerprint, response FROM idempotency_keys WHERE key = $1`, key).
		Scan(&r.Fingerprint, &r.Response)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}