package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"ledger/internal/db"
	"ledger/internal/ledger"
)

func setup(t *testing.T) (*httptest.Server, int64) {
	t.Helper()
	d, err := db.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	if err := db.Reset(context.Background(), d, "../../migrations/001_init.sql"); err != nil {
		t.Fatal(err)
	}
	ext, err := ledger.EnsureExternal(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer((&Server{DB: d}).Routes())
	t.Cleanup(ts.Close)
	return ts, ext
}

func do(t *testing.T, method, url, key string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, url, &buf)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestAPI_EndToEnd(t *testing.T) {
	ts, ext := setup(t)

	_, a := do(t, "POST", ts.URL+"/accounts", "", nil)
	_, b := do(t, "POST", ts.URL+"/accounts", "", nil)
	aid, bid := int64(a["id"].(float64)), int64(b["id"].(float64))

	// deposit: External -> A
	st, _ := do(t, "POST", ts.URL+"/transfers", "dep-1", map[string]any{"from": ext, "to": aid, "amount": 1000})
	if st != 201 {
		t.Fatalf("deposit status %d", st)
	}
	// replay: same key -> 200, same transfer
	st, r := do(t, "POST", ts.URL+"/transfers", "dep-1", map[string]any{"from": ext, "to": aid, "amount": 1000})
	if st != 200 || r["replayed"] != true {
		t.Fatalf("replay status %d body %v", st, r)
	}
	// transfer A -> B
	if st, _ = do(t, "POST", ts.URL+"/transfers", "t-1", map[string]any{"from": aid, "to": bid, "amount": 300}); st != 201 {
		t.Fatalf("transfer status %d", st)
	}
	// balances
	_, bal := do(t, "GET", ts.URL+"/accounts/"+itoa(aid)+"/balance", "", nil)
	if bal["balance"].(float64) != 700 {
		t.Errorf("A balance %v, want 700", bal["balance"])
	}
	// entries
	_, es := do(t, "GET", ts.URL+"/accounts/"+itoa(aid)+"/entries?limit=10", "", nil)
	if len(es["entries"].([]any)) != 2 {
		t.Errorf("want 2 entries for A, got %v", es["entries"])
	}
	// errors
	if st, _ = do(t, "POST", ts.URL+"/transfers", "t-2", map[string]any{"from": aid, "to": bid, "amount": 99999}); st != 422 {
		t.Errorf("insufficient funds status %d, want 422", st)
	}
	if st, _ = do(t, "POST", ts.URL+"/transfers", "", map[string]any{"from": aid, "to": bid, "amount": 1}); st != 400 {
		t.Errorf("missing key status %d, want 400", st)
	}
	if st, _ = do(t, "POST", ts.URL+"/transfers", "t-1", map[string]any{"from": aid, "to": bid, "amount": 5}); st != 409 {
		t.Errorf("key conflict status %d, want 409", st)
	}
	if st, _ = do(t, "GET", ts.URL+"/accounts/999999/balance", "", nil); st != 404 {
		t.Errorf("missing account status %d, want 404", st)
	}
}

func itoa(n int64) string { return json.Number(func() string { b, _ := json.Marshal(n); return string(b) }()).String() }