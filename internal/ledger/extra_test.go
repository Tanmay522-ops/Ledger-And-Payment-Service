package ledger

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// An expired key row must not block the key: it counts as new.
func TestSafe_ExpiredKeyIsNew(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	if _, err := d.Exec(`INSERT INTO idempotency_keys (key, fingerprint, response, expires_at)
		VALUES ($1, 'old', '{}'::jsonb, now() - interval '1 hour')`, req.Key); err != nil {
		t.Fatal(err)
	}
	res, err := Transfer(ctx, d, req)
	if err != nil {
		t.Fatalf("expired key should be accepted as new: %v", err)
	}
	if res.Replayed {
		t.Errorf("expired key must not replay")
	}
	if balance(t, d, a) != 900 || balance(t, d, b) != 100 {
		t.Errorf("want A=900 B=100, got A=%d B=%d", balance(t, d, a), balance(t, d, b))
	}
	assertBooks(t, d)
}

// Malformed requests return the right error and leave nothing behind.
func TestSafe_ValidationErrors(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	keysBefore := countRows(t, d, "idempotency_keys")
	transfersBefore := countRows(t, d, "transfers")

	cases := []struct {
		name string
		mod  func(*TransferRequest)
		want error
	}{
		{"empty key", func(r *TransferRequest) { r.Key = "" }, ErrKeyRequired},
		{"zero amount", func(r *TransferRequest) { r.Amount = 0 }, ErrInvalidAmount},
		{"negative amount", func(r *TransferRequest) { r.Amount = -5 }, ErrInvalidAmount},
		{"too large", func(r *TransferRequest) { r.Amount = MaxAmount + 1 }, ErrAmountTooLarge},
		{"same account", func(r *TransferRequest) { r.To = r.From }, ErrSameAccount},
	}
	for _, c := range cases {
		req := newTransferRequest(a, b, 100)
		c.mod(&req)
		if _, err := Transfer(ctx, d, req); !errors.Is(err, c.want) {
			t.Errorf("%s: want %v, got %v", c.name, c.want, err)
		}
	}
	if n := countRows(t, d, "idempotency_keys"); n != keysBefore {
		t.Errorf("malformed requests saved %d key rows", n-keysBefore)
	}
	if n := countRows(t, d, "transfers"); n != transfersBefore {
		t.Errorf("malformed requests created %d transfers", n-transfersBefore)
	}
	assertBooks(t, d)
}

func TestSafe_AccountNotFound(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	keysBefore := countRows(t, d, "idempotency_keys")
	if _, err := Transfer(ctx, d, newTransferRequest(a, 999999, 100)); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("want account not found, got %v", err)
	}
	if n := countRows(t, d, "idempotency_keys"); n != keysBefore {
		t.Errorf("failed request left a key row")
	}
	if balance(t, d, a) != 1000 {
		t.Errorf("balance changed")
	}
	assertBooks(t, d)
}

func TestSafe_SystemToSystemRejected(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	s1 := newAccount(t, d, "system")
	s2 := newAccount(t, d, "system")

	keysBefore := countRows(t, d, "idempotency_keys")
	if _, err := Transfer(ctx, d, newTransferRequest(s1, s2, 100)); !errors.Is(err, ErrNeedUserAccount) {
		t.Fatalf("want need-user-account, got %v", err)
	}
	if n := countRows(t, d, "idempotency_keys"); n != keysBefore {
		t.Errorf("rejected request left a key row")
	}
	assertBooks(t, d)
}

// Same key, two different contents, all at once: exactly one transfer may happen.
func TestSafe_ConcurrentKeyConflict(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	r1 := newTransferRequest(a, b, 100)
	r2 := r1
	r2.Amount = 200

	const n = 50
	ids := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req := r1
			if i%2 == 1 {
				req = r2
			}
			res, err := Transfer(ctx, d, req)
			errs[i] = err
			if err == nil {
				ids[i] = res.TransferID
			}
		}(i)
	}
	close(start)
	wg.Wait()

	var winner int64
	succeeded, conflicts := 0, 0
	for i := range errs {
		switch {
		case errs[i] == nil:
			succeeded++
			if winner == 0 {
				winner = ids[i]
			} else if ids[i] != winner {
				t.Errorf("two different transfers for one key: %d and %d", winner, ids[i])
			}
		case errors.Is(errs[i], ErrKeyConflict):
			conflicts++
		default:
			t.Errorf("unexpected error: %v", errs[i])
		}
	}
	t.Logf("succeeded/replayed=%d conflicts=%d", succeeded, conflicts)
	if succeeded == 0 || conflicts == 0 {
		t.Errorf("expected some successes and some conflicts")
	}
	if got := countRows(t, d, "transfers"); got != 2 { // funding + exactly one
		t.Errorf("transfers=%d, want 2", got)
	}
	assertBooks(t, d)
}

// A waiter gives up after the lock timeout and returns ErrInProgress.
func TestSafe_InProgressTimeout(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)

	// A "stuck" first request: claims the key and never commits.
	holder, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(`INSERT INTO idempotency_keys (key, fingerprint, response, expires_at)
		VALUES ($1, $2, '{}'::jsonb, now() + interval '24 hours')`,
		req.Key, fingerprint(req)); err != nil {
		t.Fatal(err)
	}

	t.Setenv("LEDGER_LOCK_TIMEOUT", "300ms")
	if _, err := Transfer(ctx, d, req); !errors.Is(err, ErrInProgress) {
		holder.Rollback()
		t.Fatalf("want in progress, got %v", err)
	}

	// The stuck request dies (rollback). The same key must now work.
	holder.Rollback()
	res, err := Transfer(ctx, d, req)
	if err != nil {
		t.Fatalf("retry after the stuck request died should succeed: %v", err)
	}
	if res.Replayed {
		t.Errorf("should be a fresh execution")
	}
	assertBooks(t, d)
}