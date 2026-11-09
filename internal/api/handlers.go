package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"ledger/internal/ledger"
)

type Server struct {
	DB *sql.DB
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /accounts", s.createAccount)
	mux.HandleFunc("POST /transfers", s.transfer)
	mux.HandleFunc("GET /accounts/{id}/balance", s.balance)
	mux.HandleFunc("GET /accounts/{id}/entries", s.entries)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(err, ledger.ErrKeyRequired):
		status, code = 400, "key_required"
	case errors.Is(err, ledger.ErrInvalidAmount):
		status, code = 400, "invalid_amount"
	case errors.Is(err, ledger.ErrAmountTooLarge):
		status, code = 400, "amount_too_large"
	case errors.Is(err, ledger.ErrSameAccount):
		status, code = 400, "same_account"
	case errors.Is(err, ledger.ErrNeedUserAccount):
		status, code = 400, "transfer_must_involve_user_account"
	case errors.Is(err, ledger.ErrAccountNotFound):
		status, code = 404, "account_not_found"
	case errors.Is(err, ledger.ErrInsufficientFunds):
		status, code = 422, "insufficient_funds"
	case errors.Is(err, ledger.ErrKeyConflict):
		status, code = 409, "key_conflict"
	case errors.Is(err, ledger.ErrInProgress):
		w.Header().Set("Retry-After", "1")
		status, code = 503, "in_progress"
	}
	writeJSON(w, status, map[string]string{"error": code})
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	// Only user accounts via the API (see decision log).
	id, err := ledger.CreateAccount(r.Context(), s.DB, "user")
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id, "kind": "user", "balance": 0})
}

type transferBody struct {
	From   int64 `json:"from"`
	To     int64 `json:"to"`
	Amount int64 `json:"amount"`
}

func (s *Server) transfer(w http.ResponseWriter, r *http.Request) {
	var b transferBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_json"})
		return
	}
	res, err := ledger.Transfer(r.Context(), s.DB, ledger.TransferRequest{
		Key: r.Header.Get("Idempotency-Key"), From: b.From, To: b.To, Amount: b.Amount,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	status := 201
	if res.Replayed {
		status = 200
	}
	writeJSON(w, status, map[string]any{"transfer_id": res.TransferID, "replayed": res.Replayed})
}

func pathID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil
}

func (s *Server) balance(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "invalid_id"})
		return
	}
	bal, err := ledger.GetBalance(r.Context(), s.DB, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "balance": bal})
}

func (s *Server) entries(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		writeJSON(w, 400, map[string]string{"error": "invalid_id"})
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	es, err := ledger.ListEntries(r.Context(), s.DB, id, after, limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"entries": es})
}