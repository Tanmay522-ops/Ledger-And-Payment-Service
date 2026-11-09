package ledger

import (
	"context"
	"database/sql"
	"fmt"
)

// Violation describes one broken invariant.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) String() string { return v.Invariant + ": " + v.Detail }

// Reconcile checks every invariant inside ONE read-only snapshot.
// An empty result means all rules hold.
func Reconcile(ctx context.Context, d *sql.DB) ([]Violation, error) {
	tx, err := d.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead, // one frozen view for all checks
		ReadOnly:  true,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	checks := []func(context.Context, *sql.Tx) ([]Violation, error){
		checkTransferHasTwoEntries,
		checkLedgerSumsToZero,
		checkBalancesMatchEntries,
		checkUserNotNegative,
		checkOneTransferPerKey,
		checkUserBalancesMatchSystem,
	}
	var all []Violation
	for _, c := range checks {
		v, err := c(ctx, tx)
		if err != nil {
			return nil, err
		}
		all = append(all, v...)
	}
	return all, nil
}

// Invariant 1: every transfer has exactly two entries that sum to zero.
func checkTransferHasTwoEntries(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT t.id, count(e.seq), COALESCE(sum(e.amount),0)
		FROM transfers t LEFT JOIN ledger_entries e ON e.transfer_id = t.id
		GROUP BY t.id
		HAVING count(e.seq) <> 2 OR COALESCE(sum(e.amount),0) <> 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var id, n, sum int64
		if err := rows.Scan(&id, &n, &sum); err != nil {
			return nil, err
		}
		out = append(out, Violation{"1 two-entries",
			fmt.Sprintf("transfer %d has %d entries summing to %d", id, n, sum)})
	}
	return out, rows.Err()
}

// Invariant 2: all entries in the ledger sum to zero.
func checkLedgerSumsToZero(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	var sum int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(sum(amount),0) FROM ledger_entries`).Scan(&sum); err != nil {
		return nil, err
	}
	if sum != 0 {
		return []Violation{{"2 ledger-sum", fmt.Sprintf("entries sum to %d", sum)}}, nil
	}
	return nil, nil
}

// Invariant 3: each account's stored balance equals the sum of its entries.
func checkBalancesMatchEntries(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT a.id, a.balance, COALESCE(sum(e.amount),0)
		FROM accounts a LEFT JOIN ledger_entries e ON e.account_id = a.id
		GROUP BY a.id, a.balance
		HAVING a.balance <> COALESCE(sum(e.amount),0)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var id, bal, sum int64
		if err := rows.Scan(&id, &bal, &sum); err != nil {
			return nil, err
		}
		out = append(out, Violation{"3 balance-matches-entries",
			fmt.Sprintf("account %d balance %d but entries sum to %d", id, bal, sum)})
	}
	return out, rows.Err()
}

// Invariant 4: a user account's balance is never below zero.
func checkUserNotNegative(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, balance FROM accounts WHERE kind = 'user' AND balance < 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var id, bal int64
		if err := rows.Scan(&id, &bal); err != nil {
			return nil, err
		}
		out = append(out, Violation{"4 user-not-negative",
			fmt.Sprintf("user account %d has balance %d", id, bal)})
	}
	return out, rows.Err()
}

// Invariant 6: each transfer is referenced by at most one idempotency key.
func checkOneTransferPerKey(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT transfer_id, count(*) FROM idempotency_keys
		WHERE transfer_id IS NOT NULL
		GROUP BY transfer_id HAVING count(*) > 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var id, n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out = append(out, Violation{"6 one-transfer-per-key",
			fmt.Sprintf("transfer %d is referenced by %d keys", id, n)})
	}
	return out, rows.Err()
}

// Extra: user balances equal MINUS the sum of ALL system balances.
func checkUserBalancesMatchSystem(ctx context.Context, tx *sql.Tx) ([]Violation, error) {
	var users, systems int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(sum(balance) FILTER (WHERE kind='user'),0),
		       COALESCE(sum(balance) FILTER (WHERE kind='system'),0)
		FROM accounts`).Scan(&users, &systems); err != nil {
		return nil, err
	}
	if users+systems != 0 {
		return []Violation{{"extra user-vs-system",
			fmt.Sprintf("users %d + systems %d <> 0", users, systems)}}, nil
	}
	return nil, nil
}