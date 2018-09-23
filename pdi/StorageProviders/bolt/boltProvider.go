package bolt

// See: go-plan/pdi/StorageProvider.go for interface context

import (
    "bytes"
    "encoding/base64"
    "encoding/binary"
    "os"
    "log"
    "path"
    "time"
    "sync"
    "sync/atomic"

	"github.com/plan-tools/go-plan/pdi"
	"github.com/plan-tools/go-plan/plan"

	"github.com/boltdb/bolt"
)

var (
	storageTxnBucketName = []byte("/plan/protobuf/StorageTxn")
)

const (
    boltStorageCodec  = "/plan/Storage/bolt/1"
    boltBookmarkCodec = boltStorageCodec + "/bookmark/1"
)


// provider implements pdi.StorageProvider.
// Specifically, it manages one or more bolt databases in a given local directory.
type boltProvider struct {
    pdi.StorageProvider

	sessions    []*boltSession
	dbsPathname string
	fileMode    os.FileMode
}


// NewProvider creates a bolt-based StorageProvider with the given params
func NewProvider(
	inDBsPathname string,
	inFileMode os.FileMode,
) pdi.StorageProvider {

	storage := &boltProvider{
		dbsPathname: inDBsPathname,
		fileMode:    inFileMode,
	}


	return storage
}

// StartSession implements StorageProvider.StartSession
func (provider *boltProvider) StartSession(
	inDatabaseID []byte,
) (pdi.StorageSession, error) {

	if len(inDatabaseID) < 2 || len(inDatabaseID) > 64 {
		return nil, plan.Errorf(nil, plan.InvalidDatabaseID, "got database ID of length %d", len(inDatabaseID))
	}

    err := os.MkdirAll(provider.dbsPathname, provider.fileMode)
    if err != nil {
		return nil, plan.Errorf(err, plan.FailedToAccessPath, "failed to access path %s", provider.dbsPathname)
    }

	dbName := make([]byte, base64.RawURLEncoding.EncodedLen(len(inDatabaseID)))
	base64.RawURLEncoding.Encode(dbName, inDatabaseID)

	session := &boltSession{
		parentProvider:           provider,
		outgoingChan:             make(chan *pdi.StorageMsg, 10),
        txnBatchInbox:            make(chan *txnBatch, 10),
		dbPathname:               path.Join(provider.dbsPathname, string(dbName)+".bolt"),
		maxTxnReportsBeforePause: 10,
		maxTxnBytesBeforePause:   1000000,
        //alertOutbox:              make([]pdi.StorageAlert, 0, 10),
	}

	opt := bolt.Options{
		Timeout:    2 * time.Second,
		NoGrowSync: false,
	}

	session.db, err = bolt.Open(session.dbPathname, provider.fileMode, &opt)
	if err != nil {
		return nil, plan.Errorf(err, plan.FailedToLoadDatabase, "failed to open bolt db %s", session.dbPathname)
	}

	err = session.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(storageTxnBucketName)

		return err
	})
	if err != nil {
		return nil, plan.Error(err, plan.FailedToLoadDatabase, "failed to create root bucket")
	}

	provider.sessions = append(provider.sessions, session)

    go session.readerWriter()

    session.status = pdi.SessionIsReady

    session.outgoingChan <- pdi.NewStorageAlert(pdi.SessionIsReady, "")

	return session, nil
}

/*
// SegmentIntoTxnsForCommit implements StorageProvider.SegmentIntoTxnsForCommit
func (storage *BoltStorage) SegmentIntoTxnsForCommit(
	inData []byte,
	inDataDesc pdi.TxnDataDesc,
) ([]*pdi.StorageTxn, error) {

	return pdi.SegmentIntoTxnsForMaxSize(inData, inDataDesc, storage.maxSegSize)
}
*/
func (provider *boltProvider) endSession(
    inSession *boltSession,
    msg *pdi.StorageMsg,
    ) *plan.Perror {

    if inSession.status != pdi.SessionIsReady {
        return nil
    }

    // TODO: concurrency safety analysis
	for i, session := range provider.sessions {
		if session == inSession {

            // Update the sesison status
            inSession.status = msg.AlertCode

            // Remove this session ptr from the session list
			n := len(provider.sessions) - 1
			provider.sessions[i] = provider.sessions[n]
			provider.sessions = provider.sessions[:n]
            inSession.outgoingChan <- msg

            // Cause readerWriter() loop to fire
            session.txnBatchInbox <- nil;

			return nil
		}
	}

	return plan.Error(nil, plan.InvalidStorageSession, "storage session not found")
}



type txnBatch struct {
    next            *txnBatch
	requestID     pdi.RequestID
	txnsRequested []pdi.TxnRequest

	txnsToCommit  []pdi.StorageTxn // nil if n/a
}


type boltSession struct {
    pdi.StorageSession

    status           pdi.AlertCode
	parentProvider   *boltProvider
	outgoingChan       chan *pdi.StorageMsg

    commitScrap     []byte
	dbPathname string
	db         *bolt.DB

	maxTxnReportsBeforePause int
	maxTxnBytesBeforePause   int

    txnBatchPool    sync.Pool

    txnBatchInbox  chan *txnBatch

    nextRequestID  uint32
    dbMutex sync.Mutex

    readheadPos  timeSortableKey
    readerReqID  pdi.RequestID
    readerIsActive  bool
}



// TODO shutdown readerWriter
func (session *boltSession) readerWriter() {

    session.txnBatchPool = sync.Pool{
        New: func() interface{} {
            return new(txnBatch)
        },
    }

    readerTimer := time.NewTicker(time.Millisecond * 25)
    wakeTimer := readerTimer.C

    for session.IsReady() {
       
        // When the session is catching up on reads, set wakeTimer to non-nil
        if session.readerIsActive {
            wakeTimer = readerTimer.C
        } else {
            wakeTimer = nil
        }

        // FIRST, choose requests made via RequestTxns() and CommitTxns(), THEN make incremental progress frorm the read head.
        select {
            case batch := <- session.txnBatchInbox:

                // Sending a nil batch means we're shutting down
                if batch != nil {

                    session.doTxn(batch)

                    // We're done with this item, so make it available for reuse
                    session.txnBatchPool.Put(batch)

                }

            case <-wakeTimer:
                session.doTxn(nil)
        }

    }

    
}




func (session *boltSession) readerStep(tx *bolt.Tx, bucket *bolt.Bucket) []pdi.StorageTxn {

    txns := make([]pdi.StorageTxn, session.maxTxnReportsBeforePause)
        
    bytesProcessed := 0

    c := bucket.Cursor()
    var err error
    count := 0

    for curKey, txnBuf := c.Seek(session.readheadPos[:]); count < len(txns); curKey, txnBuf = c.Next() {

        if curKey == nil {
            session.readerIsActive = false
            break
        }
        copy(session.readheadPos[:], curKey)

        bytesProcessed += len(txnBuf)

        txn := &txns[count]

        err = txn.UnmarshalWithOptionalBody(txnBuf, true)
        if err != nil {
            err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn.Unmarhal() failed")
        } else {
            if ! session.readheadPos.Equals(txn.TimeCommitted, txn.TxnName) {
                err = plan.Errorf(err, plan.FailedToUnmarshalTxn, "StorageTxn verification failed for txn %v (expected %v)", txn.TxnName, session.readheadPos[:])
            }
        }

        if err != nil {

            // TODO: log to pnode error channel instead
            log.Fatal(err)
        }

        session.readheadPos.Increment()

        count++
        if bytesProcessed > session.maxTxnBytesBeforePause {
            break;
        }
    }


    return txns[:count]
}


func (session *boltSession) readTxns(tx *bolt.Tx, bucket *bolt.Bucket, txnsRequested []pdi.TxnRequest) []pdi.StorageTxn {

    N := len(txnsRequested)
    txns := make([]pdi.StorageTxn, N)

    {
        for i, req := range txnsRequested {

            txn := &txns[i]

            // Lookup the txn (speedy since its keyed by time)
            timeKey := formTimeSortableKey(req.TimeCommitted, req.TxnName)
            txnBuf := bucket.Get(timeKey[:])
            if txnBuf == nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_INVALID_TXN

                // TODO: add log msg?
                continue
            }

            err := txn.UnmarshalWithOptionalBody(txnBuf, req.IncludeBody)
            if err != nil {
                err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn.Unmarhal() failed")
            } else {
                if txn.TimeCommitted != req.TimeCommitted || bytes.Compare(txn.TxnName, req.TxnName) != 0 {
                    err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn verification failed")
                }
            }

            if err != nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_LOST

                // TODO: add log msg?
                continue
            }
        }
    }

    return txns
}



func (session *boltSession) commitTxns(tx *bolt.Tx, bucket *bolt.Bucket, txns []pdi.StorageTxn) error {

    N := len(txns)
    var txnBuf []byte

    curTime := plan.Now().UnixSecs

    var err error

    for i := 0; i < N; i++ {

        txn := &txns[i]

        txn.TimeCommitted = curTime
        txn.TimeConsensus = curTime 

        // In a local db like this, once the entry is written, it won't ever revert (and we need to set this before marshalling)
        txn.TxnStatus = pdi.TxnStatus_FINALIZED

        var commitID uint64
        commitID, err = bucket.NextSequence()
        if err != nil {
            break
        }

        timeKey := formTimeSortableKeyWithID(txn.TimeCommitted, commitID)
        txn.TxnName = timeKey[8:]

        txnBuf, err = txn.MarshalForOptionalBody(txnBuf)

        if err == nil {
            err = bucket.Put(timeKey[:], txnBuf)
        }

        // Regardless of if the commit success, nil out each txn body so the StorageTxns sent back don't include the body
        txn.Body = nil

        // If a txn errors, now (and the below will be called)
        if err != nil {
            break
        }
    }

    return err
}





func (session *boltSession) doTxn(
    txnOp *txnBatch,
    ) {

    msg := pdi.NewStorageMsg()

    isWriteOp := false

    if txnOp == nil {
        msg.StorageOp = pdi.OpTxnReport
        msg.RequestID = session.readerReqID
    } else {
        msg.RequestID = txnOp.requestID

        txnsToCommit  := len(txnOp.txnsToCommit)
        txnsRequested := len(txnOp.txnsRequested)
        plan.Assert(txnsToCommit == 0 || txnsRequested == 0, "txnBatch assert failed")
        
        if txnsToCommit > 0 {
            msg.Txns = txnOp.txnsToCommit
            msg.StorageOp = pdi.OpCommitTxn
            isWriteOp = true
        } else if txnsRequested > 0 {
            msg.StorageOp = pdi.OpRequestTxns
        }
    }

    session.dbMutex.Lock()

    tx, dbErr := session.db.Begin(isWriteOp)
    var bucket *bolt.Bucket
    if dbErr == nil {
		bucket = tx.Bucket(storageTxnBucketName)
        if bucket == nil {
            dbErr = plan.Errorf(dbErr, plan.FailedToCommitTxn, "storage bucket '%s' not found", storageTxnBucketName)
        }
    }

    if dbErr == nil {
        switch msg.StorageOp {
            case pdi.OpCommitTxn:
                dbErr = session.commitTxns(tx, bucket, txnOp.txnsToCommit)
            case pdi.OpRequestTxns:
                msg.Txns = session.readTxns(tx, bucket,txnOp.txnsRequested)
            case pdi.OpTxnReport:
                msg.Txns = session.readerStep(tx, bucket)

        }

        // If we encountered an error, rollback the db (commit none) -- otherwise, commit.
        if dbErr != nil || ! isWriteOp {
            tx.Rollback()
        } else if (msg.StorageOp == pdi.OpCommitTxn) {
            dbErr = tx.Commit()
            
            if dbErr != nil {
                dbErr = plan.Error(dbErr, plan.FailedToCommitTxn, "bolt.Tx.Commit() failed")

                // If the commmit failed, update the status only for commit msgs
                msg.AlertCode = pdi.CommitFailed

                // If we get an error for any of the sub txns, fail them all
                for _, txn := range msg.Txns {
                    txn.Body = nil
                    txn.TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
                }
            }
        }
    }

    session.dbMutex.Unlock()

    if dbErr != nil {
        if msg.AlertCode == 0 {
            msg.AlertCode = pdi.StorageFailure
        }
        if len(msg.AlertMsg) == 0 {
            msg.AlertMsg = dbErr.Error()
        }
    }

    session.outgoingChan <- msg
}

/*
func (session *boltSession) readBurst() {

    // Create the return msg
    msg := pdi.StorageMsg{
        RequestID: readerReqID,
        Txns: make([]*StorageTxn, session.maxTxnReportsBeforePause),
    }
    
    tx, err := session.db.Begin(false)
    var bucket *bolt.Bucket
    if err == nil {
		bucket = tx.Bucket(storageTxnBucketName)
        if bucket == nil {
            err = plan.Errorf(err, plan.FailedToCommitTxn, "storage bucket '%s' not found", storageTxnBucketName)
        }
    }

    if err != nil {
        msg.AlertMsg  = err.Error()
        msg.AlertCode = pdi.StorageFailure
        msg.Txns      = nil
    } else {
    
        for i, req:= range inBatch.txnsRequested {

            txn := &pdi.StorageTxn{}
            msg.Txns[i] = txn

            // Lookup the txn (speedy since its keyed by time)
            timeKey := pdi.FormTimeSortableKey(req.TimeCommitted, req.TxnName)
            txnBuf := bucket.Get(timeKey[:])
            if txnBuf == nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_INVALID_TXN

                // TODO: add log msg?
                continue
            }

            err = txn.UnmarshalWithOptionalBody(txnBuf, req.IncludeBody)
            if err != nil {
                err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn.Unmarhal() failed")
            } else {
                if txn.TimeCommitted != req.TimeCommitted || bytes.Compare(txn.TxnName, req.TxnName) != 0 {
                    err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn verificiation failed")
                }
            }

            if err != nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_LOST

                // TODO: add log msg?
                continue
            }
        }

// **close DB!


        // If one of the txns failed, don't cause an alert
        err = nil
    }

    // Send the result message back to the client session
    session.outgoingChan <- msg


}
*/

/*

	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil



    N := len(inTxnNames)
    session.txnReqsMux.Lock()
    for i := range inTxnNames {
        session.txnReqs.Push(inTxnNames[i], inOmitData)
    }
    session.txnReqsMux.Unlock()

    txnReqSignal <- true

}


unc (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

                msg.AlertCode = pdi.FailedToCommitTxn
                msg.AlertMsg  = err.Error()

                session.msgOutbox = append(session.msgOutbox, msg)
                log.Println(alert)

                return alert






func (session *boltSession) commitBatch(inBatch *txnBatch) {

    if len(inBatch.txnsToCommit) == 0 {
        return
    }

    msg := pdi.StorageMsg{
        RequestID: inBatch.requestID,
        Txns: inBatch.txnsToCommit,
    }
    
    tx, err := session.db.Begin(true)
    var bucket *bolt.Bucket
    if err == nil {
		bucket = tx.Bucket(storageTxnBucketName)
        if bucket == nil {
            err = plan.Errorf(err, plan.FailedToCommitTxn, "storage bucket '%s' not found", storageTxnBucketName)
        }
    }

    if err != nil {
        msg.AlertMsg  = err.Error()
        msg.AlertCode = pdi.StorageFailure
        msg.Txns      = nil
    } else {
        msg.Txns, dbErr = commitTxns(tx, bucket, msg.Txns)
    }

    if tx != nil {
        // If we encountered an error, rollback the db (commit none) -- otherwise, commit.
        if err != nil {
            tx.Rollback()
        } else {
            err = tx.Commit()
            
            if err != nil {
                err = plan.Error(err, plan.FailedToCommitTxn, "bolt Commit() failed")
            }
        }
    }

    // Send the result message back to the client session
    {
        for _, txn := range msg.Txns {

            // Regardless of if the commit success, nil out each txn so the StorageTxns sent back only carry status
            txn.Body = nil

            // If the commit failed, mark each txn as failed
            if err != nil {
                txn.TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
            }
        }

        if err != nil {
            msg.AlertMsg = err.Error()
            msg.AlertCode = pdi.CommitFailed
        }

        session.msgsOut <- msg
    }

}




func (session *boltSession) readBatch(inBatch *txnBatch) {

    if len(inBatch.txnsRequested) == 0 {
        return
    }

    // Create the return msg
    msg := pdi.StorageMsg{
        RequestID: inBatch.requestID,
        Txns: make([]*pdi.StorageTxn, len(inBatch.txnsRequested)),
    }

    var bucket *bolt.Bucket
    tx, err := session.db.Begin(false)
    if err == nil {
		bucket = tx.Bucket(storageTxnBucketName)
        if bucket == nil {
            err = plan.Errorf(err, plan.FailedToCommitTxn, "storage bucket %s not found", storageTxnBucketName)
        }
    }
 
    if err != nil {
        msg.AlertMsg  = err.Error()
        msg.AlertCode = pdi.StorageFailure
        msg.Txns      = nil
    } else {
    
        for i, req:= range inBatch.txnsRequested {

            txn := &pdi.StorageTxn{}
            msg.Txns[i] = txn

            // Lookup the txn (speedy since its keyed by time)
            timeKey := pdi.FormTimeSortableKey(req.TimeCommitted, req.TxnName)
            txnBuf := bucket.Get(timeKey[:])
            if txnBuf == nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_INVALID_TXN

                // TODO: add log msg?
                continue
            }

            err = txn.UnmarshalWithOptionalBody(txnBuf, req.IncludeBody)
            if err != nil {
                err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn.Unmarhal() failed")
            } else {
                if txn.TimeCommitted != req.TimeCommitted || bytes.Compare(txn.TxnName, req.TxnName) != 0 {
                    err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn verificiation failed")
                }
            }

            if err != nil {
                txn.TxnName       = req.TxnName
                txn.TimeCommitted = req.TimeCommitted
                txn.TxnStatus     = pdi.TxnStatus_LOST

                // TODO: add log msg?
                continue
            }
        }

// **close DB!


        // If one of the txns failed, don't cause an alert
        err = nil
    }

    // Send the result message back to the client session
    session.outgoingChan <- msg
}


*/

func (session *boltSession) startNewRequest() *txnBatch {

    batch := session.txnBatchPool.Get().(*txnBatch)
    batch.txnsRequested = batch.txnsRequested[:0]
    batch.txnsToCommit  = batch.txnsToCommit[:0]
    batch.requestID     = pdi.RequestID(atomic.AddUint32(&session.nextRequestID, 1))
    
    return batch
}



func (session *boltSession) IsReady() bool {
    return session != nil && session.status == pdi.SessionIsReady
}

func (session *boltSession) GetOutgoingChan() <-chan *pdi.StorageMsg {
    return session.outgoingChan
}



func (session *boltSession) RequestTxns(
	inTxnRequests []pdi.TxnRequest,
    ) (pdi.RequestID, error) {

    if ! session.IsReady() {
        return 0, plan.Error(nil, plan.SessionNotReady, "storage session not ready for RequestTxns()")
    }
  
    batch := session.startNewRequest()
    batch.txnsRequested = append(batch.txnsRequested, inTxnRequests[:]...)
    
    session.txnBatchInbox <- batch
    
    return batch.requestID, nil
}


func (session *boltSession) ReportFromBookmark(
    inFromBookmark *plan.Block,
    ) (pdi.RequestID, error) {

    if ! session.IsReady() {
        return 0, plan.Error(nil, plan.SessionNotReady, "storage session not ready for ReportFromBookmark()")
    }

    var readheadPos timeSortableKey
    if inFromBookmark != nil {
        bookmark := inFromBookmark.GetContentWithCodec(boltBookmarkCodec, 0)
        if bookmark == nil || len(bookmark) != timeSortableKeySz {
            return 0, plan.Errorf(nil, plan.FailedToUnmarshal, "Error unmarshalling inFromBookmark")
        }
        copy(readheadPos[:], bookmark)
    } else {
        readheadPos = formTimeSortableKeyWithID(0, 0)
    }

    session.readerIsActive = true

    // This will cause the readerWriter loop to fire and use the wake timer
    session.txnBatchInbox <- nil

    session.readerReqID = pdi.RequestID(atomic.AddUint32(&session.nextRequestID, 1))

    return session.readerReqID, nil

}

func (session *boltSession) GetBookmark() (*plan.Block, error) {

    bookmark := &plan.Block{
        Codec: boltBookmarkCodec,
        Content: session.readheadPos[:],
    }

    return bookmark, nil

}


func (session *boltSession) CommitTxn(
    inTxnBody *plan.Block,
    ) (pdi.RequestID, error) {

    if ! session.IsReady() {
        return 0, plan.Error(nil, plan.SessionNotReady, "storage session not ready for CommitTxns()")
    }


    batch := session.startNewRequest()
    batch.txnsToCommit = append(batch.txnsToCommit, pdi.StorageTxn{
        TxnStatus: pdi.TxnStatus_COMMITTING,
        Body: inTxnBody,
    })
        
    session.txnBatchInbox <- batch
    
    return batch.requestID, nil

}




func (session *boltSession) EndSession(inReason string) {

    session.dbMutex.Lock()
    {
        if session.db != nil {
            session.db.Close()
            session.db = nil
        }

        session.parentProvider.endSession(session, pdi.NewStorageAlert(pdi.SessionEndedByClient, inReason)) 
    }
    session.dbMutex.Unlock()

}








// storageTxnNameSz is sizeof(bolt.Bucket.NextSequence())
const storageTxnNameSz = 8

// timeSortableKeySz reflects the size of a uint6 time index plus the size of a StorageTxnName
const timeSortableKeySz = 8 + storageTxnNameSz

// timeSortableKey is concatenation of a 8-byte unix timestamp followed by a StorageTxnName.
type timeSortableKey [timeSortableKeySz]byte


func formTimeSortableKey(inTime int64, inTxnName []byte) timeSortableKey {
	var k timeSortableKey

	binary.BigEndian.PutUint64(k[0:8], uint64(inTime))

	overhang := storageTxnNameSz - len(inTxnName)
	if overhang < 0 {
		copy(k[8:], inTxnName[-overhang:])
	} else {
		copy(k[8+overhang:], inTxnName)
	}

	return k
}


func formTimeSortableKeyWithID(inTime int64, inTxnName uint64) timeSortableKey {
	var k timeSortableKey

	binary.BigEndian.PutUint64(k[0:8], uint64(inTime))
	binary.BigEndian.PutUint64(k[8:16], inTxnName)

	return k
}

func (tk *timeSortableKey) Equals(inTime int64, inTxnName []byte) bool {
    if binary.BigEndian.Uint64(tk[0:8]) != uint64(inTime) {
        return false
    }
    if bytes.Compare(tk[8:16], inTxnName) != 0 {
        return false
    }

    return true
}



// Increment "adds 1" to a TimeSortableKey, alloing a search routine to increment to the next possible hashname.
// Purpose is to increment.  E.g.
//   ... 39 00 => ... 39 01
//   ... 39 01 => ... 39 02
//            ...
//   ... 39 ff => ... 3A 00
func (tk *timeSortableKey) Increment() {

	// pos says which byte-significan't digit we're on -- start at the least signigicant
	pos := timeSortableKeySz - 1
	for {

		// Increment and stop if there's no carry
		tk[pos]++
		if tk[pos] > 0 {
			break
		}

		// We're here because there's a carry -- so move to the next byte digit
		pos--
	}
}

/*

// This allows efficient time-based sorting of StorageTxn hashnames.  For a hash collision to occur with these sizes,
//     the txns would have to occur during the *same* second AND be 1 in 10^77 (AND would have to occur in the *same* communty)




// FormTimeSortableKey lays out a unix timestamp in 8 bytes in big-endian followed by and a TimeSortableKey (a hashname).
// This gives these time+hashname keys (and their accompanying value) the property to be stored, sorted by timestamp.
func FormTimeSortableKey(inTime int64, inTxnName []byte) TimeSortableKey {
	var k TimeSortableKey

	binary.BigEndian.PutUint64(k[0:8], uint64(inTime))

	overhang := StorageTxnNameSz - len(inTxnName)
	if overhang < 0 {
		copy(k[8:], inTxnName[-overhang:])
	} else {
		copy(k[8+overhang:], inTxnName)
	}

	return k
}








func (session *boltSession) ReadTxns(
    inTxnNames [][]byte,
    ) ([]pdi.StorageTxn, error) {

    txns := make([]pdi.StorageTxn, len(inTxnNames))

    err := session.db.View( func(tx *bolt.Tx) error {
        bucket := tx.Bucket(storageTxnBucketName)

        for i, txnName := range inTxnNames {
            txnBuf := bucket.Get(txnName)
            txns[i].TxnName = txnName
            if txnBuf == nil {
                txns[i].TxnStatus = pdi.TxnStatus_INVALID_TXN
                continue
            }

            err := txns[i].Unmarshal(txnBuf)

            // Reset the name since it was overwritten by Unmarshal() above
            txns[i].TxnName = txnName

            if err != nil {
                txns[i].TxnStatus = pdi.TxnStatus_LOST      // TODO, add log msg!?
                continue
            }
        }

        return nil
    })

    if err != nil {
        return nil, err
    }

    return txns, nil
}




     for i, txnName := range inTxnNames {
            bucketGet(key []byte) []byte

        for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
            fmt.Printf("key=%s, value=%s\n", k, v)
        }

        b := tx.Bucket([]byte("MyBucket"))
        v := b.Get([]byte("answer"))

    })


func (session *boltSession) StartReporting(
    inFromTime int64,
    inFromTimeWithData int64,
    ) {

}



func (session *boltSession) CommitTxns(
    inTxns []pdi.StorageTxn,
    ) error {

    curTime := plan.Now().UnixSecs

    err := session.db.Update( func(tx *bolt.Tx) error {
        bucket := tx.Bucket(storageTxnBucketName)

        for i := range inTxns {

            inTxns[i].TimeCommitted = curTime

            // In a db like this, once the entry is written, it won't ever revert
            inTxns[i].TxnStatus = pdi.TxnStatus_FINALIZED

            txnBuf, err := inTxns[i].Marshal()
            if err != nil {
                return plan.Error(err, plan.FailedToCommitTxn, "error marshalling StorageTxn")
            }

            err = bucket.Put(inTxns[i].TxnName, txnBuf)
            if err != nil {
                return plan.Error(err, plan.FailedToCommitTxn, "error putting bolt key-value")
            }
        }

        return nil
    })

    return err

}





*/


/*
type txnBatchQueue struct {
	items	[]*txnBatch
	head	int
	tail	int
	count	int
}

// Push adds a node to the queue.
func (q *txnBatchQueue) Push(item *txnBatch) {
	if q.head == q.tail && q.count > 0 {
		items := make([]*txnBatch, len(q.items)*2 + 8)
		copy(items, q.items[q.head:])
		copy(items[len(q.items)-q.head:], q.items[:q.head])
		q.head = 0
		q.tail = len(q.items)
		q.items = items
	}
	q.items[q.tail] = item
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
}

// Pop removes and returns a node from the queue in first to last order.
func (q *txnBatchQueue) Pop() *txnBatch {
	if q.count == 0 {
		return nil
	}
	item := q.items[q.head]
	q.head = (q.head + 1) % len(q.items)
	q.count--
	return item
}
*/

