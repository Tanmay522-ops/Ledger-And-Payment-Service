package ledger

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Same scenario as the naive race: A has 1000, each transfer is 100.
func TestSafe_Race(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	var ok, declined, other int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := Transfer(ctx, d, newTransferRequest(a, b, 100))
			switch {
			case err == nil:
				atomic.AddInt64(&ok, 1)
			case errors.Is(err, ErrInsufficientFunds):
				atomic.AddInt64(&declined, 1)
			default:
				atomic.AddInt64(&other, 1)
				t.Logf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	balA, balB := balance(t, d, a), balance(t, d, b)
	t.Logf("succeeded=%d declined=%d other=%d  A=%d  B=%d", ok, declined, other, balA, balB)
	if ok != 10 || declined != 90 || other != 0 {
		t.Errorf("want 10 ok / 90 declined / 0 other")
	}
	if balA != 0 || balB != 1000 {
		t.Errorf("want A=0 B=1000, got A=%d B=%d", balA, balB)
	}
	assertBooks(t, d)
}

func TestSafe_RetrySequential(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	var first int64
	for i := 0; i < 50; i++ {
		res, err := Transfer(ctx, d, req)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = res.TransferID
		} else if !res.Replayed || res.TransferID != first {
			t.Fatalf("attempt %d: want replay of transfer %d, got %+v", i, first, res)
		}
	}
	if got := balance(t, d, a); got != 900 {
		t.Errorf("A=%d, want 900", got)
	}
	assertBooks(t, d)
}

func TestSafe_RetryConcurrent(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make([]int64, 50)
	errs := make([]error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := Transfer(ctx, d, req)
			errs[i] = err
			if err == nil {
				ids[i] = res.TransferID
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Errorf("request %d got transfer %d, request 0 got %d", i, ids[i], ids[0])
		}
	}
	var n int
	d.QueryRow(`SELECT count(*) FROM transfers WHERE from_account=$1`, a).Scan(&n)
	t.Logf("transfers created=%d  A=%d", n, balance(t, d, a))
	if n != 1 {
		t.Errorf("want exactly 1 transfer, got %d", n)
	}
	if got := balance(t, d, a); got != 900 {
		t.Errorf("A=%d, want 900", got)
	}
	assertBooks(t, d)
}

// A->B and B->A at the same time must finish, not deadlock.
func TestSafe_OpposingTransfers(t *testing.T) {
	d := setupSafe(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)
	fundSafe(t, d, ext, b, 1000)

	var wg sync.WaitGroup
	var failed int64
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		from, to := a, b
		if i%2 == 1 {
			from, to = b, a
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := Transfer(ctx, d, newTransferRequest(from, to, 10)); err != nil {
				atomic.AddInt64(&failed, 1)
				t.Logf("error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if failed != 0 {
		t.Errorf("%d transfers failed", failed)
	}
	if balance(t, d, a) != 1000 || balance(t, d, b) != 1000 {
		t.Errorf("want A=1000 B=1000, got A=%d B=%d", balance(t, d, a), balance(t, d, b))
	}
	assertBooks(t, d)
}

// A decline is remembered: after a deposit, the SAME key still replays the decline.
func TestSafe_DeclineIsReplayed(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 50)

	req := newTransferRequest(a, b, 100)
	if _, err := Transfer(ctx, d, req); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("want insufficient funds, got %v", err)
	}
	fundSafe(t, d, ext, a, 1000) // now she could afford it
	if _, err := Transfer(ctx, d, req); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("same key must replay the decline, got %v", err)
	}
	if _, err := Transfer(ctx, d, newTransferRequest(a, b, 100)); err != nil { // new key works
		t.Fatalf("new key should succeed: %v", err)
	}
	assertBooks(t, d)
}

// Same key, different contents -> key conflict.
func TestSafe_KeyConflict(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	if _, err := Transfer(ctx, d, req); err != nil {
		t.Fatal(err)
	}
	req.Amount = 200
	if _, err := Transfer(ctx, d, req); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("want key conflict, got %v", err)
	}
}

// The database backstop is back ON: a direct bad write is refused.
func TestSafe_BackstopConstraint(t *testing.T) {
	d := setupSafe(t)
	a := newAccount(t, d, "user")
	if _, err := d.Exec(`UPDATE accounts SET balance = -5 WHERE id = $1`, a); err == nil {
		t.Fatal("database should refuse a negative user balance")
	}
}