package pdi

import (
	"fmt"

	"github.com/plan-tools/go-plan/plan"
)

/*****************************************************
** StorageProvider
**/

// StorageProvider wraps a persistent storage (or replicating) service "producer".  Perhaps it's locally implemented, or perhaps its implemented by gRPC.
type StorageProvider interface {

	// StartSession initiates a new session with a given db/repo identifier (typically a UUID or hashname)
	StartSession(
		inDatabaseID []byte,
		inTxnReportChannel chan<- TxnReport,
		iAlertChannel chan<- StorageAlert,
		inOnCompletion func(StorageSession, error),
	) error

	// SegmentForCommit is a utility that segments and packages the given payload into one or more foundational units of storage.
	// This encapsulates nuances with replicator/storage implementations where there are txn size limits, etc.
	SegmentIntoTxnsForCommit(
		inData []byte,
		inDataDesc TxnDataDesc,
	) ([]StorageTxn, error)
}

// TxnRequestID allows a StorageSession client to identify TxnReports as they arrive from the session
type TxnRequestID uint32

// TxnReport is emitted from a StorageSession, initiated by
type TxnReport struct {
	RequestID TxnRequestID
	Txns      []*StorageTxn // READ-ONLY
}

/*****************************************************
** StorageSession
**/

// StorageSession wraps a persistent storage (or replicating) service "consumer"
// All calls in this interface are implemented as threadsafe.
type StorageSession interface {

	// IsReady reports if this session is open and ready to receive requests
	IsReady() bool

	// ReportTxns requests that the given txn names to be added to the report stream.  If a txn name is unknown or invalid, then StorageTxn.TxnStatus is set to INVALID_TXN.
	ReportTxns(inTxnNames [][]byte, inOmitData bool) (TxnRequestID, error)

	// ReportFromBookmark sets the session's metaphorical read head based on state information returned via GetBookmark() from this or a previous session.
	ReportFromBookmark(inFromBookmark plan.Block) (TxnRequestID, error)

	// GetBookmark returns an opaque, StorageProvider-specifc blob of state information that a client uses for StartReporting().
	GetBookmark() (*plan.Block, error)

	// CommitTxns submits the given finished entry to the storage implementation for publishing.
	CommitTxns(inTxns []StorageTxn) (TxnRequestID, error)

	// EndSession ends this session, resulting in the sessions parent provider signal the session's end.
	// Following a call to EndSession(), no more references to this session should be made -- StorageProvider.StartSession() must be called again.
	EndSession(inReason string)
}

/*****************************************************
** StorageAlert
**/

// StorageAlert identifies a specific warning, error, or unfolding situation occuring with a StorageProvider
type StorageAlert struct {
	AlertCode AlertCode
	Msg       string       // Human-readable amplifying information
	Txn       *StorageTxn  // If not applicable, nil
	RequestID TxnRequestID // If not applicable, set to 0
}

func (alert *StorageAlert) Error() string {
	return fmt.Sprintf("%s {code:%d}", alert.Msg, alert.AlertCode)
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

	// FailedToCommitTxn a txn failed to commit (and generally means the StorageProvider is having issues)
	FailedToCommitTxn

	//

)

/*****************************************************
** Utils
**/

// SegmentIntoTxnsForMaxSize is a utility that chops up a payload buffer into segments <= inMaxSegmentSize
func SegmentIntoTxnsForMaxSize(
	inData []byte,
	inDataDesc TxnDataDesc,
	inMaxSegmentSize int,
) ([]*StorageTxn, error) {

	bytesRemain := len(inData)
	pos := 0

	N := (len(inData) + inMaxSegmentSize - 1) / inMaxSegmentSize
	txns := make([]*StorageTxn, 0, N)

	for bytesRemain > 0 {

		segSz := bytesRemain
		if segSz < inMaxSegmentSize {
			segSz = inMaxSegmentSize
		}

		txns = append(txns, &StorageTxn{
			TxnStatus:   TxnStatus_AWAITING_COMMIT,
			DataDesc:    inDataDesc,
			SegmentData: inData[pos:segSz],
		})

		pos += segSz
	}

	for i, txn := range txns {
		txn.SegmentNum = uint32(i)
		txn.TotalSegments = uint32(len(txns))
	}

	return txns, nil

	//if bytesRemain != 0 {
	//    return plan.Error(nil, plan.AssertFailure, "assertion failed in SegmentPayloadForSegmentSize {N:%d, bytesRemain:%d}", N, bytesRemain)
	//}
}
