package datastore

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/plan-systems/go-plan/plan"
)

// DsAccess is used to wrap a Datastore AND ds.Txn
type DsAccess interface {
	ds.Read
	ds.Write
}

// TxnHelper wraps ds.Txn for easier Commit() reattempts
type TxnHelper struct {
	DsAccess DsAccess
	ds       ds.Datastore

	// Private
	maxAttempts int
	readOnly    bool
	dsTxn       ds.Txn // May be nil if this ds isn't a TxnDatastore
	isDone      bool
	commitErr   error
	fatalErr    error
	tryNum      int
}

// NewTxnHelper returns a helper struct that wraps datastore.Txn for convenience.
// This makes life easier for reattempting txns if they go stale, etc.
func (St *Store) NewTxnHelper() TxnHelper {
	return TxnHelper{
		ds:          St.ds,
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
	if h.DsAccess != nil {
		panic("BeginTry/EndTry mismatch")
	}

	if h.fatalErr != nil || h.isDone {
		return false
	}

	if h.tryNum >= h.maxAttempts {
		h.fatalErr = plan.Errorf(h.commitErr, plan.StorageNotReady, "datastore txn retry limit exceeded (%v)", h.maxAttempts)
		return false
	}

	// If we have a TxnDatastore, make a new txn, otherwise we use the Datastore interface
	txnDs, ok := h.ds.(ds.TxnDatastore)
	if txnDs != nil && ok {
		var err error
		h.dsTxn, err = txnDs.NewTransaction(h.readOnly)
		if err != nil {
			h.fatalErr = plan.Errorf(err, plan.StorageNotReady, "txnDatastore.NewTransaction() failed")
		}
		h.DsAccess = h.dsTxn
	} else {
		h.DsAccess = h.ds
	}

	if h.fatalErr != nil {
		h.dsTxn = nil
		h.DsAccess = nil
		return false
	}

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

	if h.DsAccess != nil {

		// If h.dsTxn == nil, then this ds isn't a TxnDatastore
		if h.dsTxn == nil {
			h.isDone = true
		} else {

			if inFatalErr == nil {
				err := h.dsTxn.Commit()
				if err == nil {
					h.isDone = true
				} else {
					h.commitErr = plan.Error(err, plan.StorageNotReady, "ds.Txn.Commit() failed")
				}
			} else {

				// TODO: what causes a txn to go stale or fail?
				h.dsTxn.Discard()
			}
		}

		h.dsTxn = nil
		h.DsAccess = nil
	}
}

// FatalErr returns the most recent error passed to Finish() *or* an error reflecting that the max number of retries was reached.
// If non-nil, this reflects that the txn was NOT committed.
func (h *TxnHelper) FatalErr() error {
	return h.fatalErr
}
