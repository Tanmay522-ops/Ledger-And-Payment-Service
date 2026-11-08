package ledger

import "errors"

type TransferRequest struct {
	Key    string
	From   int64
	To     int64
	Amount int64
}

type TransferResult struct {
	TransferID int64
	Replayed   bool
}

var ErrInsufficientFunds = errors.New("insufficient funds")


const MaxAmount = 100_000_000 // paise (Rs 10 lakh)

var (
	ErrKeyRequired     = errors.New("key required")
	ErrInvalidAmount   = errors.New("invalid amount")
	ErrAmountTooLarge  = errors.New("amount too large")
	ErrSameAccount     = errors.New("same account")
	ErrAccountNotFound = errors.New("account not found")
	ErrNeedUserAccount = errors.New("transfer must involve a user account")
	ErrKeyConflict     = errors.New("key conflict")
	ErrInProgress      = errors.New("in progress")
)