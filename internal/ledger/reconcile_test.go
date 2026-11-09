package ledger

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

func TestReconcile_CleanLedgerPasses(t *testing.T) {
	d := setupSafe(t)
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	b := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)
	if _, err := Transfer(context.Background(), d, newTransferRequest(a, b, 300)); err != nil {
		t.Fatal(err)
	}
	assertBooks(t, d)
}

// The check must be able to FAIL: corrupt a balance directly and expect detection.
func TestReconcile_DetectsCorruptedBalance(t *testing.T) {
	d := setupSafe(t)
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	if _, err := d.Exec(`UPDATE accounts SET balance = balance + 5 WHERE id = $1`, a); err != nil {
		t.Fatal(err)
	}
	v, err := Reconcile(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) == 0 {
		t.Fatal("reconciliation missed a corrupted balance")
	}
	found := false
	for _, x := range v {
		if strings.HasPrefix(x.Invariant, "3") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected invariant 3 violation, got %v", v)
	}
}

// A transfer with only ONE entry (a simulated half-update) must be detected.
func TestReconcile_DetectsHalfTransfer(t *testing.T) {
	d := setupSafe(t)
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	var tid int64
	if err := d.QueryRow(`INSERT INTO transfers (from_account,to_account,amount)
		VALUES ($1,$2,100) RETURNING id`, a, ext).Scan(&tid); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(`INSERT INTO ledger_entries (transfer_id,account_id,amount)
		VALUES ($1,$2,-100)`, tid, a); err != nil {
		t.Fatal(err)
	}
	v, _ := Reconcile(context.Background(), d)
	if len(v) == 0 {
		t.Fatal("reconciliation missed a half-written transfer")
	}
}

// Invariant 5: the database refuses to edit or delete ledger entries.
func TestReconcile_LedgerIsAppendOnly(t *testing.T) {
	d := setupSafe(t)
	ext := newAccount(t, d, "system")
	a := newAccount(t, d, "user")
	fundSafe(t, d, ext, a, 1000)

	if _, err := d.Exec(`UPDATE ledger_entries SET amount = 1`); err == nil {
		t.Error("UPDATE on ledger_entries should be refused")
	}
	if _, err := d.Exec(`DELETE FROM ledger_entries`); err == nil {
		t.Error("DELETE on ledger_entries should be refused")
	}
	assertBooks(t, d)
}

// Randomized: many random transfers, concurrently; the books must always balance.
func TestReconcile_RandomizedRun(t *testing.T) {
	d := setupSafe(t)
	ctx := context.Background()
	ext := newAccount(t, d, "system")
	users := make([]int64, 5)
	for i := range users {
		users[i] = newAccount(t, d, "user")
		fundSafe(t, d, ext, users[i], 500)
	}

	rng := rand.New(rand.NewSource(42)) // fixed seed: reproducible
	type job struct{ from, to, amt int64 }
	jobs := make([]job, 200)
	for i := range jobs {
		f, to := users[rng.Intn(5)], users[rng.Intn(5)]
		if f == to {
			to = ext // deposit/withdraw path too
		}
		jobs[i] = job{f, to, int64(rng.Intn(150) + 1)}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			<-start
			Transfer(ctx, d, newTransferRequest(j.from, j.to, j.amt)) // declines are fine
		}(j)
	}
	close(start)
	wg.Wait()
	assertBooks(t, d)
}