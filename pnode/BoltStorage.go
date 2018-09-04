package pnode

// See: go-plan/pdi/StorageProvider.go for interface context

import (
	"bytes"
	"encoding/base64"
    "encoding/binary"
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

// BoltStorage implements pdi.StorageProvider.
// Specifically, it manages one or more bolt databases in a given local directory.
type BoltStorage struct {
    pdi.StorageProvider

	sessions    []*boltSession
	dbsPathname string
	fileMode    os.FileMode
	maxSegSize  int
}

// NewBoltStorage creates a bolt-based StorageProvider with the given params
func NewBoltStorage(
	inDBsPathname string,
	inFileMode os.FileMode,
) *BoltStorage {

	storage := &BoltStorage{
		dbsPathname: inDBsPathname,
		fileMode:    inFileMode,
		maxSegSize:  1000000,
	}

	return storage
}

// StartSession implements StorageProvider.StartSession
func (storage *BoltStorage) StartSession(
	inDatabaseID []byte,
    inMsgChannel chan<- pdi.StorageMsg,
    inOnCompletion func(pdi.StorageSession, error),
) error {

	if len(inDatabaseID) < 2 || len(inDatabaseID) > 64 {
		return plan.Errorf(nil, plan.InvalidDatabaseID, "got database ID of length %d", len(inDatabaseID))
	}

	var dbName []byte
	base64.RawURLEncoding.Encode(dbName, inDatabaseID)

	session := &boltSession{
		parentProvider:           storage,
		msgsOut:                  inMsgChannel,
        txnBatchInbox:            make(chan *txnBatch, 8),
		dbPathname:               path.Join(storage.dbsPathname, string(dbName)+".bolt"),
		maxTxnReportsBeforePause: 20,
		maxTxnBytesBeforePause:   1000000,
        //alertOutbox:              make([]pdi.StorageAlert, 0, 10),
	}

	opt := bolt.Options{
		Timeout:    2 * time.Second,
		NoGrowSync: false,
	}

	var err error
	session.db, err = bolt.Open(session.dbPathname, storage.fileMode, &opt)
	if err != nil {
		return plan.Errorf(err, plan.FailedToLoadDatabase, "failed to open bolt db %s", session.dbPathname)
	}

	err = session.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(storageTxnBucketName)

		return err
	})
	if err != nil {
		return plan.Error(err, plan.FailedToLoadDatabase, "failed to create root bucket")
	}

	storage.sessions = append(storage.sessions, session)

    session.status = pdi.SessionIsReady
	go inOnCompletion(session, nil)

	return nil
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
func (storage *BoltStorage) endSession(inSession *boltSession, msg pdi.StorageMsg) *plan.Perror {

    if inSession.status != pdi.SessionIsReady {
        return nil
    }

    // TODO: concurrency safety analysis
	for i, session := range storage.sessions {
		if session == inSession {
			n := len(storage.sessions) - 1
			storage.sessions[i] = storage.sessions[n]
			storage.sessions = storage.sessions[:n]
            inSession.msgsOut <- msg
			return nil
		}
	}

	return plan.Error(nil, plan.InvalidStorageSession, "storage session not found")
}



type txnBatch struct {
	requestID     pdi.RequestID
	txnsRequested []pdi.TxnRequest

	txnsToCommit  []*pdi.StorageTxn // nil if n/a
}


type boltSession struct {
    pdi.StorageSession

    status           pdi.AlertCode
	parentProvider   *BoltStorage
	msgsOut       chan<- pdi.StorageMsg

	dbPathname string
	db         *bolt.DB

	maxTxnReportsBeforePause int
	maxTxnBytesBeforePause   int

    txnBatchPool    sync.Pool

	txnBatchInbox    chan *txnBatch

    nextRequestID  uint32

	//txnBatchQueue txnBatchQueue
}




func (session *boltSession) readerWriter() {

    session.txnBatchPool = sync.Pool{
        New: func() interface{} {
            return new(txnBatch)
        },
    }

    for {

        // TODO: pol 

        batch := <- session.txnBatchInbox

        // If nil is sent, it means it's time to exit
        if batch == nil {
            return
        }

        session.commitBatch(batch)

        session.reportBatch(batch)

        // We're done with this item, so make it available for reuse
        session.txnBatchPool.Put(batch)
    }

}
/*

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
*/




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
 
        var txnBuf []byte

	    curTime := plan.Now().UnixSecs

        for i, txn := range inBatch.txnsToCommit {

			txn.TimeCommitted = curTime
            txn.TimeConsensus = curTime 

			// In a local db like this, once the entry is written, it won't ever revert (and we need to set this before marshalling)
			txn.TxnStatus = pdi.TxnStatus_FINALIZED
            
            txnBuf, err = txn.MarshalForOptionalBody(txnBuf)

			if err == nil {
                timeKey := marshalTimeSortableKey(txn.TimeCommitted, txn.TxnName)
                err = bucket.Put(timeKey[:], txnBuf)
            }

            // If a txn errors, stop where we're at rollback
			if err != nil {
                break
            }
        }


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




func (session *boltSession) reportBatch(inBatch *txnBatch) {

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
            timeKey := marshalTimeSortableKey(req.TimeCommitted, req.TxnName)
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




func (session *boltSession) startNewRequest() *txnBatch {

    batch := session.txnBatchPool.Get().(*txnBatch)
    batch.txnsRequested = batch.txnsRequested[:0]
    batch.txnsToCommit  = batch.txnsToCommit[:0]
    batch.requestID     = pdi.RequestID(atomic.AddUint32( &session.nextRequestID, 1))
    
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
    inTxns []*pdi.StorageTxn,
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




func marshalTimeSortableKey(inTime int64, inTxnName []byte) [32]byte {
    var k [32]byte

    binary.BigEndian.PutUint64(k[0:8], uint64(inTime))

	overhang := pdi.StorageTxnNameSz - len(inTxnName)
	if overhang < 0 {
        copy(k[8:], inTxnName[-overhang:])
	} else {
        copy(k[8+overhang:], inTxnName)
    }

	return k
}