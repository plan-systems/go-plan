package datastore

import (
    "github.com/dgraph-io/badger"
	"github.com/plan-systems/go-plan/plan"
)


// TxnHelper wraps ds.Txn for easier Commit() reattempts
type TxnHelper struct {
	Txn      *badger.Txn

	// Private
    db          *badger.DB
	maxAttempts int
	readOnly    bool
	isDone      bool
	commitErr   error
	fatalErr    error
	tryNum      int
}

// NewTxnHelper returns a helper struct that wraps datastore.Txn for convenience.
// This makes life easier for reattempting txns if they go stale, etc.
func NewTxnHelper(db *badger.DB) TxnHelper {
	return TxnHelper{
		db:          db,
		maxAttempts: 5,
		readOnly:    false,
		tryNum:      0,
	}
}

// NextAttempt is intended to be used to control a for loop and returns true only if:
//     1) the max number of attempts has not been reached
//     2) Finish() has not been called with a non-nil error
//     3) the txn has not yet been committed successfully yet.
func (h *TxnHelper) NextAttempt() bool {
	if h.Txn != nil {
		panic("BeginTry/EndTry mismatch")
	}

	if h.fatalErr != nil || h.isDone {
		return false
	}

	if h.tryNum >= h.maxAttempts {
		h.fatalErr = plan.Errorf(h.commitErr, plan.StorageNotReady, "datastore txn retry limit exceeded (%v)", h.maxAttempts)
		return false
	}

	h.Txn = h.db.NewTransaction(!h.readOnly)

	h.tryNum++
	return true
}

// Finish is called with inFatalErr == nil to denote that the ds.Txn should be committed.  If the commit
//     succeeds, the next call to NextAttempt() will have no effect and return false.  If the commit
//     fails (b/c the txn got stale), NextAttempt() will return true until the max number of attempts has been reached.
// If inFatalErr != nil, then the current ds.Txn.Discard() is called  and the next call to NextAttempt() will return false.
func (h *TxnHelper) Finish(inFatalErr error) {

	// Note, this will overwrite any prev set fatal err
	if inFatalErr != nil {
		h.fatalErr = inFatalErr
	}

	if h.Txn != nil {

        if inFatalErr == nil {
            err := h.Txn.Commit()
            if err == nil {
                h.isDone = true
            } else {
                h.commitErr = plan.Error(err, plan.StorageNotReady, "badger.Txn.Commit() failed")
            }
        }
        
        h.Txn.Discard()
		h.Txn = nil
	}
}

// FatalErr returns the most recent error passed to Finish() *or* an error reflecting that the max number of retries was reached.
// If non-nil, this reflects that the txn was NOT committed.
func (h *TxnHelper) FatalErr() error {
	return h.fatalErr
}
