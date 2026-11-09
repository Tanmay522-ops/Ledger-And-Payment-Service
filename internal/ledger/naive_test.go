//go:build naive

package ledger

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Race: A has 1000, each transfer is 100, so exactly 10 may succeed.
func TestNaive_Race(t *testing.T) {
	d := setupNaive(t)
	NaiveDelay = 2 * time.Millisecond
	defer func() { NaiveDelay = 0 }()
	ctx := context.Background()

	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fund(t, d, ext, a, 1000)

	var ok int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines at the same moment
			if _, err := NaiveTransfer(ctx, d, newTransferRequest(a, b, 100)); err == nil {
				atomic.AddInt64(&ok, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	balA, balB := balance(t, d, a), balance(t, d, b)
	t.Logf("succeeded=%d  A=%d  B=%d", ok, balA, balB)
	if ok != 10 {
		t.Errorf("expected exactly 10 successes, got %d", ok)
	}
	if balA < 0 {
		t.Errorf("A went negative: %d", balA)
	}
}

// Retry, sequential: the naive key check should survive this one.
func TestNaive_RetrySequential(t *testing.T) {
	d := setupNaive(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fund(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	for i := 0; i < 50; i++ {
		if _, err := NaiveTransfer(ctx, d, req); err != nil {
			t.Fatal(err)
		}
	}
	if got := balance(t, d, a); got != 900 {
		t.Errorf("A=%d, want 900", got)
	}
}

// Retry, concurrent: same key, 50 requests at once.
func TestNaive_RetryConcurrent(t *testing.T) {
	d := setupNaive(t)
	NaiveDelay = 2 * time.Millisecond
	defer func() { NaiveDelay = 0 }()
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fund(t, d, ext, a, 1000)

	req := newTransferRequest(a, b, 100)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			NaiveTransfer(ctx, d, req)
		}()
	}
	close(start)
	wg.Wait()

	var n int
	d.QueryRow(`SELECT count(*) FROM transfers WHERE from_account=$1`, a).Scan(&n)
	t.Logf("transfers created=%d  A=%d", n, balance(t, d, a))
	if n != 1 {
		t.Errorf("expected exactly 1 transfer for one key, got %d", n)
	}
}