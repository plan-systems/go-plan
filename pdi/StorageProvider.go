

package pdi



// StorageAlert identifies a specific warning, error, or situation with a StorageProvider 
type StorageAlert int32

// StorageAlertHandler is a callback proc for when there is a StorageAlert
type StorageAlertHandler func(inAlert StorageAlert)




// StorageProvider wraps a persistent storage (or replicating) service "producer".  Perhaps it's locally implemented, or perhaps its implemented by gRPC.
type StorageProvider interface {

    // StartSession initiates a new session with a given db/repo identifier (typically a UUID or hashname) 
    StartSession(
        inDatabaseID []byte,
        inOnSessionStarted func(inSession StorageSession, inErr error),
        inOnTxnReport func(inTxns []*StorageTxn),
        inOnSessionEnded func(inReason string),
        //inAlertHandler StorageAlertHandler,
    ) error


    // SegmentForCommit is a utility that segments and packages the given payload into one or more foundational units of storage.
    // This encapsulates nuances with replicator/storage implementations where there are txn size limits, etc. 
    SegmentIntoTxnsForCommit(
        inData []byte, 
        inDataDesc TxnDataDesc,
    ) ([]StorageTxn, error)
}




// StorageSession wraps a persistent storage (or replicating) service "consumer"
// All calls in this interface are threadsafe.
type StorageSession interface {

    // ReportTxns requests that the given txn names to be added to the report stream.  If a txn name is unknown or invalid, then StorageTxn.TxnStatus is set to INVALID_TXN. 
    ReportTxns(inTxnNames [][]byte, inOmitData bool)

    // StartReporting sets the metaphorical read head, causing inOnTxnReport() to receive txn reports onward from the given time index.
    StartReporting(inFromTime, inFromTimeWithData int64)

    // CommitTxns submits the given finished entry to the storage implementation for publishing.   
    CommitTxns(inTxns []StorageTxn) error

    // EndSession ends this session, resulting in the sessions parent provider to call its inOnSessionEnded() callback.
    // Following a call to EndSession(), no more references to this session should be made -- StorageProvider.StartSession() must be called again.
    EndSession(inReason string)
}









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

        txns = append( txns, &StorageTxn {
            TxnStatus: TxnStatus_NOT_COMMITTED,
            DataDesc :inDataDesc,
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

