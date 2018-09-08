package bolt

// See: go-plan/pdi/StorageProvider.go for interface context

import (
    "bytes"
    "encoding/base64"
    "os"
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

// provider implements pdi.StorageProvider.
// Specifically, it manages one or more bolt databases in a given local directory.
type boltProvider struct {
    pdi.StorageProvider

	sessions    []*boltSession
	dbsPathname string
	fileMode    os.FileMode
}


// NewBoltProvider creates a bolt-based StorageProvider with the given params
func NewBoltProvider(
	inDBsPathname string,
	inFileMode os.FileMode,
) *boltProvider {

	storage := &boltProvider{
		dbsPathname: inDBsPathname,
		fileMode:    inFileMode,
	}

	return storage
}

// StartSession implements StorageProvider.StartSession
func (provider *boltProvider) StartSession(
	inDatabaseID []byte,
    inMsgChannel chan<- pdi.StorageMsg,
) (pdi.StorageSession, error) {

	if len(inDatabaseID) < 2 || len(inDatabaseID) > 64 {
		return nil, plan.Errorf(nil, plan.InvalidDatabaseID, "got database ID of length %d", len(inDatabaseID))
	}

	var dbName []byte
	base64.RawURLEncoding.Encode(dbName, inDatabaseID)

	session := &boltSession{
		parentProvider:           provider,
		msgsOut:                  inMsgChannel,
        txnBatchInbox:            make(chan *txnBatch, 8),
		dbPathname:               path.Join(provider.dbsPathname, string(dbName)+".bolt"),
		maxTxnReportsBeforePause: 10,
		maxTxnBytesBeforePause:   1000000,
        //alertOutbox:              make([]pdi.StorageAlert, 0, 10),
	}

	opt := bolt.Options{
		Timeout:    2 * time.Second,
		NoGrowSync: false,
	}

	var err error
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

    session.status = pdi.SessionIsReady

    inMsgChannel <- pdi.StorageMsg{
        AlertCode: pdi.SessionIsReady,
    }

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
func (provider *boltProvider) endSession(inSession *boltSession, msg pdi.StorageMsg) *plan.Perror {

    if inSession.status != pdi.SessionIsReady {
        return nil
    }

    // TODO: concurrency safety analysis
	for i, session := range provider.sessions {
		if session == inSession {
			n := len(provider.sessions) - 1
			provider.sessions[i] = provider.sessions[n]
			provider.sessions = provider.sessions[:n]
            inSession.msgsOut <- msg
			return nil
		}
	}

	return plan.Error(nil, plan.InvalidStorageSession, "storage session not found")
}



type txnBatch struct {
	requestID     pdi.RequestID
	txnsRequested []pdi.TxnRequest

	txnsToCommit  []pdi.StorageTxn // nil if n/a
}


type boltSession struct {
    pdi.StorageSession

    status           pdi.AlertCode
	parentProvider   *boltProvider
	msgsOut       chan<- pdi.StorageMsg

	dbPathname string
	db         *bolt.DB

	maxTxnReportsBeforePause int
	maxTxnBytesBeforePause   int

    txnBatchPool    sync.Pool

	txnBatchInbox    chan *txnBatch
    nextRequestID  uint32

    readheadPos  pdi.TimeSortableKey
    readerReqID  pdi.RequestID
	readerEvent    chan int32


	//txnBatchQueue txnBatchQueue
}




func (session *boltSession) readerWriter() {

    session.txnBatchPool = sync.Pool{
        New: func() interface{} {
            return new(txnBatch)
        },
    }

    for {
        //var batch *txnBatch

        // FIRST, choose requests made via RequestTxns() and CommitTxns(), THEN make incremental progress frorm the read head.
        select {
            case batch := <- session.txnBatchInbox:

                // Sending a nil batch means we're shutting down
                if batch == nil {
                    return
                }

                session.doTxn(batch)

                // We're done with this item, so make it available for reuse
                session.txnBatchPool.Put(batch)

            default:

                // When the readhead is catchin up to the current membnt
                sleepMS := <-session.readerEvent

                if ( sleepMS >= 0 ) {
                    time.Sleep(time.Millisecond * time.Duration(sleepMS))
                }

                session.doTxn(nil)
        }

        

        // If nil is sent, it means it's time to exit
    

    }

}




func (session *boltSession) readStep(tx *bolt.Tx, bucket *bolt.Bucket) []pdi.StorageTxn {

    txns := make([]pdi.StorageTxn, session.maxTxnReportsBeforePause)
        
    bytesProcessed := 0

    c := bucket.Cursor()
    var err error
    count := 0

    for curKey, txnBuf := c.Seek(session.readheadPos[:]); curKey != nil && count < len(txns); curKey, txnBuf = c.Next() {

        bytesProcessed += len(txnBuf)

        txn := &txns[count]

        err = txn.UnmarshalWithOptionalBody(txnBuf, true)
        if err != nil {
            err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn.Unmarhal() failed")
        } else {
            session.readheadPos = pdi.FormTimeSortableKey(txn.TimeCommitted, txn.TxnName)
            if bytes.Compare(session.readheadPos[:], curKey) != 0 {
                err = plan.Error(err, plan.FailedToUnmarshalTxn, "StorageTxn verification failed")
            }
        }

        if err != nil {
            // TODO send an alert
        }

        session.readheadPos.Increment()

        txnCount++
        if bytesProcessed > session.maxTxnBytesBeforePause {
            break;
        }
    }

    return txns[:txnCount]
}


func (session *boltSession) readTxns(tx *bolt.Tx, bucket *bolt.Bucket, txnsRequested []pdi.TxnRequest) []pdi.StorageTxn {

    N := len(txnsRequested)
    txns := make([]pdi.StorageTxn, N)

    {
        for i, req := range txnsRequested {

            txn := &txns[i]

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
        
        txnBuf, err = txn.MarshalForOptionalBody(txnBuf)

        if err == nil {
            timeKey := pdi.FormTimeSortableKey(txn.TimeCommitted, txn.TxnName)
            err = bucket.Put(timeKey[:], txnBuf)
        }

        // Regardless of if the commit success, nil out each txn body so the StorageTxns sent back don't include the body
        txn.Body = nil

        // If a txn errors, now (and the below will be called)
        if err != nil {
            return err
        }
    }

    return err
}





func (session *boltSession) doTxn(
    txnOp *txnBatch,
    ) {

    msg := pdi.StorageMsg{
        RequestID: txnOp.requestID,
    }


    if txnOp == nil {
        msg.StorageOp = pdi.OpTxnReport
    } else {
        txnsToCommit  := len(txnOp.txnsToCommit)
        txnsRequested := len(txnOp.txnsRequested)
        plan.Assert(txnsToCommit == 0 || txnsRequested == 0, "txnBatch assert failed")
        
        if txnsToCommit > 0 {
            msg.Txns = txnOp.txnsToCommit
            msg.StorageOp = pdi.OpCommitTxns
        } else if txnsRequested > 0 {
            msg.StorageOp = pdi.OpRequestTxns
        }
    }

    tx, dbErr := session.db.Begin(msg.StorageOp == pdi.OpCommitTxns)
    var bucket *bolt.Bucket
    if dbErr == nil {
		bucket = tx.Bucket(storageTxnBucketName)
        if bucket == nil {
            dbErr = plan.Errorf(dbErr, plan.FailedToCommitTxn, "storage bucket '%s' not found", storageTxnBucketName)
        }
    }

    if dbErr == nil {
        switch msg.StorageOp {
            case pdi.OpCommitTxns:
                dbErr = session.commitTxns(tx, bucket, txnOp.txnsToCommit)
            case pdi.OpRequestTxns:
                msg.Txns = session.readTxns(tx, bucket,txnOp.txnsRequested)
            case pdi.OpTxnReport:
                msg.Txns = session.readStep(tx, bucket)

        }

        // If we encountered an error, rollback the db (commit none) -- otherwise, commit.
        if dbErr != nil {
            tx.Rollback()
        } else {
            dbErr = tx.Commit()
            
            if dbErr != nil {
                dbErr = plan.Error(dbErr, plan.FailedToCommitTxn, "bolt.Tx.Commit() failed")

                // If the commmit failed, update the status only for commit msgs
                if dbErr != nil {
                    switch msg.StorageOp {
                        case pdi.OpCommitTxns:
                            msg.AlertCode = pdi.CommitFailed

                            // If we get an error for any of the sub txns, fail them all
                            for _, txn := range msg.Txns {
                                txn.Body = nil
                                txn.TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
                            }
                    }
                }
            }
        }
    }


    if dbErr != nil {
        if msg.AlertCode == 0 {
            msg.AlertCode = pdi.StorageFailure
        }
        if len(msg.AlertMsg) == 0 {
            msg.AlertMsg = dbErr.Error()
        }
    }

    session.msgsOut <- msg
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
    session.msgsOut <- msg


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
    session.msgsOut <- msg
}


*/

func (session *boltSession) startNewRequest() *txnBatch {

    batch := session.txnBatchPool.Get().(*txnBatch)
    batch.txnsRequested = batch.txnsRequested[:0]
    batch.txnsToCommit  = batch.txnsToCommit[:0]
    batch.requestID     = pdi.RequestID(atomic.AddUint32(&session.nextRequestID, 1))
    
    return batch
}



func (session *boltSession) RequestTxns(
	inTxnRequests []pdi.TxnRequest,
    ) (pdi.RequestID, error) {

    if session.status != pdi.SessionIsReady {
        return 0, plan.Error(nil, plan.SessionNotReady, "storage session not ready for RequestTxns()")
    }
  
    batch := session.startNewRequest()
    batch.txnsRequested = append(batch.txnsRequested, inTxnRequests[:]...)
    
    session.txnBatchInbox <- batch
    
    return batch.requestID, nil
}




func (session *boltSession) CommitTxns(
    inTxns []pdi.StorageTxn,
    ) (pdi.RequestID, error) {

    if session.status != pdi.SessionIsReady {
        return 0, plan.Error(nil, plan.SessionNotReady, "storage session not ready for CommitTxns()")
    }

    batch := session.startNewRequest()
    batch.txnsToCommit = append(batch.txnsToCommit, inTxns[:]...)
    
    session.txnBatchInbox <- batch
    
    return batch.requestID, nil

}



/*



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



func (session *boltSession) EndSession(inReason string) {

    if session.db != nil {
        session.db.Close()
    }

    session.parentProvider.endSession(session, pdi.StorageAlert{
                AlertCode: pdi.SessionEnded,
                Info: inReason,
            })
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

