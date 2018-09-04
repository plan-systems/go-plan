package pdi

import (
	"fmt"

	"github.com/plan-tools/go-plan/plan"
)

/*****************************************************
** StorageProvider
**/

// StorageProvider wraps a persistent storage service "producer".  The StorageProvider+StorageSession model is designed
//    to wrap ANY kind of append-only database, particularly a blockchain.  Further this interface design allows the
//    provider to be remote (and serve requests via RPC).
type StorageProvider interface {

	// StartSession initiates a new session with a given db/repo identifier (typically a UUID or hashname)
	StartSession(
		inDatabaseID []byte,
		inMsgChannel chan<- StorageMsg,
		inOnCompletion func(StorageSession, error),
	) error
}

/*****************************************************
** StorageSession
**/

// StorageSession wraps a persistent storage (or replicating) service "consumer"
// All calls in this interface are implemented as threadsafe.
type StorageSession interface {

	// IsReady reports if this session is open and ready to receive requests
	IsReady() bool

	// RequestTxns requests that the given txn names to be added to the msg stream.  If a txn name is unknown or invalid, then StorageTxn.TxnStatus is set to INVALID_TXN.
	RequestTxns(inTxnRequests []TxnRequest) (RequestID, error)

	// RequestFromBookmark sets the session's "read head position" based on state information returned via GetBookmark() from this or a previous session.
	RequestFromBookmark(inFromBookmark plan.Block) (RequestID, error)

	// GetBookmark returns an opaque, StorageProvider-specifc blob of state information that a client uses for StartReporting().
	GetBookmark() (*plan.Block, error)

	// CommitTxns submits the given finished entry to the storage implementation for publishing.
	CommitTxns(inTxns []*StorageTxn) (RequestID, error)

	// EndSession ends this session, resulting in the sessions parent provider signal the session's end.
	// Following a call to EndSession(), no more references to this session should be made -- StorageProvider.StartSession() must be called again.
	EndSession(inReason string)
}

// RequestID allows a StorageSession client to identify/match StorageReports as they arrive from the StorageProvider.
type RequestID uint32

// TxnRequest requests a given txn by name and commit time (both are required)
// If LoadBody == false, then StorageTxn.TxnBody will be nil (saving bandwidth)
type TxnRequest struct {
	TxnName       []byte
	TimeCommitted int64
	IncludeBody   bool
}

/*****************************************************
** StorageMsg
**/

// StorageMsg serves two purposes:
//    (a) transport requested StorageTxns (and their status) to StorageSession clients
//    (b) sending important storage alerts to storage clients (e.g. txn failures or system shutdown warnings)
type StorageMsg struct {
	RequestID RequestID     // Set to 0 if n/a
	Txns      []*StorageTxn // READ-ONLY for clients once committed.  These txns have finished processing (successfully or unsuccessfully)
	AlertCode AlertCode     // Set to 0 if n/a or no alert given
	AlertMsg  string        // Human-readable amplifying information
}

func (alert *StorageMsg) Error() string {
	return fmt.Sprintf("%s {code:%d}", alert.AlertMsg, alert.AlertCode)
}

// AlertCode allows a StorageAlert to be easily classified
type AlertCode int32

const (

	// SessionIsReady means this StorageSession is ready for requests
	SessionIsReady AlertCode = 5900 + iota

	// SessionWillEnd means this StorageSession will end soon, described in StorageEvent.Info
	SessionWillEnd

	// SessionEndedByClient means this StorageSession was ended via StorageSession.EndSession()
	SessionEndedByClient

	// SessionEndedByProvider means this StorageSession was ended by host StorageProvider
	SessionEndedByProvider

	// SessionErroredOut means this StorageSession ended due to an error
	SessionErroredOut

	// StorageFailure means database access failed in an unexpected way
	StorageFailure

	// CommitFailed means the given CommitTxns() request failed
	CommitFailed
)
