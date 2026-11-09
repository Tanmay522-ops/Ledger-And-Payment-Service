package ledger

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
)

var errInjected = errors.New("injected failure")

func countRows(t *testing.T, d *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := d.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Failure between debit and credit: nothing may be left half-done.
func TestSafe_CrashBetweenDebitAndCredit(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	transfersBefore := countRows(t, d, "transfers")
	entriesBefore := countRows(t, d, "ledger_entries")
	keysBefore := countRows(t, d, "idempotency_keys")

	afterDebitHook = func() error { return errInjected }
	defer func() { afterDebitHook = nil }()

	req := newTransferRequest(a, b, 100)
	_, err := Transfer(ctx, d, req)
	if !errors.Is(err, errInjected) {
		t.Fatalf("want injected failure, got %v", err)
	}

	if got := balance(t, d, a); got != 1000 {
		t.Errorf("A=%d, want 1000 (debit must be undone)", got)
	}
	if got := balance(t, d, b); got != 0 {
		t.Errorf("B=%d, want 0", got)
	}
	if n := countRows(t, d, "transfers"); n != transfersBefore {
		t.Errorf("transfers rows %d, want %d", n, transfersBefore)
	}
	if n := countRows(t, d, "ledger_entries"); n != entriesBefore {
		t.Errorf("ledger_entries rows %d, want %d", n, entriesBefore)
	}
	if n := countRows(t, d, "idempotency_keys"); n != keysBefore {
		t.Errorf("idempotency_keys rows %d, want %d (key claim must be undone)", n, keysBefore)
	}
	assertBooks(t, d)

	// The failure is gone: the SAME key can now be retried and must work, exactly once.
	afterDebitHook = nil
	res, err := Transfer(ctx, d, req)
	if err != nil {
		t.Fatalf("retry after failure should succeed: %v", err)
	}
	if res.Replayed {
		t.Errorf("retry should be a fresh execution, not a replay")
	}
	if balance(t, d, a) != 900 || balance(t, d, b) != 100 {
		t.Errorf("want A=900 B=100, got A=%d B=%d", balance(t, d, a), balance(t, d, b))
	}
	assertBooks(t, d)
}

// Same idea, but the failure comes in the middle of many concurrent transfers.
func TestSafe_CrashRandomUnderLoad(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 100000)

	var n int64
	var mu sync.Mutex
	afterDebitHook = func() error {
		mu.Lock()
		defer mu.Unlock()
		n++
		if n%3 == 0 { // every third transfer fails mid-way
			return errInjected
		}
		return nil
	}
	defer func() { afterDebitHook = nil }()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			Transfer(ctx, d, newTransferRequest(a, b, 100))
		}()
	}
	close(start)
	wg.Wait()

	// Whatever mix of successes and failures happened, the books must balance.
	assertBooks(t, d)
	ok := int64(countRows(t, d, "transfers")) - 1 // minus the funding transfer
	if got := balance(t, d, b); got != ok*100 {
		t.Errorf("B=%d but %d transfers committed", got, ok)
	}
}