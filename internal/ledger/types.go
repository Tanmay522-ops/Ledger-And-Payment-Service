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