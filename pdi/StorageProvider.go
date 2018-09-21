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
	) (StorageSession, error)

}

/*****************************************************
** StorageSession
**/

// StorageSession wraps a persistent storage (or replicating) service "consumer"
// All calls to this interface are implemented to be THREADSAFE.
type StorageSession interface {

	// IsReady reports if this session is open and ready to receive requests
	IsReady() bool

    // GetOutgoingChan returns the chan all outgoing StorageMsgs appear on.
    GetOutgoingChan() <-chan StorageMsg

	// RequestTxns requests that the given txn names to be added to the msg stream.  If a txn name is unknown or invalid, then StorageTxn.TxnStatus is set to INVALID_TXN.
	RequestTxns(inTxnRequests []TxnRequest) (RequestID, error)

	// ReportFromBookmark sets the session's "read head position" based on state information returned via GetBookmark() from this or a previous session.
	ReportFromBookmark(inFromBookmark *plan.Block) (RequestID, error)

	// GetBookmark returns an opaque, StorageProvider-specific blob of state information that a client uses for StartReporting().
	GetBookmark() (*plan.Block, error)

	// CommitTxn submits the given block to the storage implementation for publishing.  Once the inTxnBody has been committed,
	//    a corresponding StorageTxn will appear in the session's StorageMsg channel with status.
	CommitTxn(inTxnBody *plan.Block) (RequestID, error)

	// EndSession ends this session, resulting in the sessions parent provider signal the session's end.
	// Following a call to EndSession(), no more references to this session should be made -- StorageProvider.StartSession() must be called again.
	EndSession(inReason string)
}

// RequestID allows a StorageSession client to identify/match StorageReports as they arrive from the StorageProvider.
// A valid RequestID is always >= 0
type RequestID uint32

// TxnRequest requests a given txn by name and commit time (both are required)
// If IncludeBody == false, then StorageTxn.TxnBody will be nil (saving bandwidth)
type TxnRequest struct {
	TxnName       []byte
	TimeCommitted int64
	IncludeBody   bool
}

/*****************************************************
** StorageMsg
**/

// StorageMsg serves two top-level purposes:
//    (a) transport requested StorageTxns (and their status) to StorageSession clients
//    (b) sending important storage alerts to storage clients (e.g. txn failures or system shutdown warnings)
type StorageMsg struct {
	RequestID RequestID // Set to 0 if n/a
	StorageOp StorageOp
	Txns      []StorageTxn // READ-ONLY for clients once committed.  These txns have finished processing (successfully or unsuccessfully)
	AlertCode AlertCode    // Set to 0 if n/a or no alert given
	AlertMsg  string       // Human-readable amplifying information
}

// StorageOp describes what kind of storage operation is/was happening
type StorageOp int32

const (

	// OpCommitTxn means the given txn are/were to be committed (and this StorageMsg is reporting status)
	OpCommitTxn = 1 + iota

	// OpRequestTxns means the given txns are/were requested
	OpRequestTxns

	// OpTxnReport means this the given txns have been updated or are the next batch of txns resulting from ReportFromBookmark()
	OpTxnReport
)

func (alert *StorageMsg) Error() string {
	return fmt.Sprintf("%s {code:%d}", alert.AlertMsg, alert.AlertCode)
}

// AlertCode allows a StorageAlert to be easily classified
type AlertCode int32

const (

	/*****************************************************
	** SessionInfoAlerts
	**/

	// SessionInfoAlertMask is used to filter alerts that are informative and don't represent direct concern
	SessionInfoAlertMask AlertCode = 0x0100 + iota

	// SessionIsReady means this StorageSession is ready for requests
	SessionIsReady

	// SessionWillEnd means this StorageSession will end soon, described in StorageMsg.AlertMsg
	SessionWillEnd

    // TxnReportsUpToDate means the txn readhead is now up to date and any new txn will show up as 
    TxnReportsUpToDate


	/*****************************************************
	** SessionFailureAlerts
	**/

	// SessionFailureAlertMask is used to filter alerts that reflect a falure or problem happening
	SessionFailureAlertMask AlertCode = 0x0200 + iota

	// StorageFailure means database access failed in an unexpected way
	StorageFailure

	// CommitFailed means the given CommitTxns() request failed
	CommitFailed

	/*****************************************************
	** SessionEndedAlerts
	**/

	// SessionEndedAlertMask is used to filter alerts that means the given StorageSession has ended (and is not ready)
	SessionEndedAlertMask AlertCode = 0x0400 + iota

	// SessionEndedByClient means this StorageSession was ended via StorageSession.EndSession()
	SessionEndedByClient

	// SessionEndedByProvider means this StorageSession was ended by host StorageProvider
	SessionEndedByProvider

	// SessionErroredOut means this StorageSession ended due to an error
	SessionErroredOut
)
