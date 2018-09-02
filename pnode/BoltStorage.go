package pnode

// See: go-plan/pdi/StorageProvider.go for interface context

import (
	"encoding/base64"
	"os"
	"path"
	"time"

	//"sync"

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
	inTxnReportChannel chan<- pdi.TxnReport,
	inAlertHandler func(inAlert pdi.StorageAlert),
	inOnCompletion func(pdi.StorageSession, error),
) error {

	if len(inDatabaseID) < 2 || len(inDatabaseID) > 64 {
		return plan.Errorf(nil, plan.InvalidDatabaseID, "got database ID of length %d", len(inDatabaseID))
	}

	var dbName []byte
	base64.RawURLEncoding.Encode(dbName, inDatabaseID)

	session := &boltSession{
		parentProvider:           storage,
		txnReportChannel:         inTxnReportChannel,
		alertHandler:             inAlertHandler,
		dbPathname:               path.Join(storage.dbsPathname, string(dbName)+".bolt"),
		maxTxnReportsBeforePause: 20,
		maxTxnBytesBeforePause:   1000000,
        alertOutbox:              make([]pdi.StorageAlert, 0, 10),
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


// SegmentIntoTxnsForCommit implements StorageProvider.SegmentIntoTxnsForCommit
func (storage *BoltStorage) SegmentIntoTxnsForCommit(
	inData []byte,
	inDataDesc pdi.TxnDataDesc,
) ([]*pdi.StorageTxn, error) {

	return pdi.SegmentIntoTxnsForMaxSize(inData, inDataDesc, storage.maxSegSize)
}

func (storage *BoltStorage) endSession(inSession *boltSession, alert pdi.StorageAlert) *plan.Perror {

    if inSession.status != pdi.SessionIsReady {
        return nil
    }

    // TODO: concurrency safety analysis
	for i, session := range storage.sessions {
		if session == inSession {
			n := len(storage.sessions) - 1
			storage.sessions[i] = storage.sessions[n]
			storage.sessions = storage.sessions[:n]
            inSession.alertHandler(alert)
			return nil
		}
	}

	return plan.Error(nil, plan.InvalidStorageSession, "storage session not found")
}



type txnBatch struct {
	requestID       pdi.TxnRequestID
	txnsToReport    [][]byte // nil if n/a
	reportOmitsData bool

	txnsToCommit []*pdi.StorageTxn // nil if n/a
}


type boltSession struct {
    pdi.StorageSession

    status           pdi.AlertCode
	parentProvider   *BoltStorage
	txnReportChannel chan<- pdi.TxnReport
	alertHandler     func(inAlert pdi.StorageAlert)

	dbPathname string
	db         *bolt.DB

    alertOutbox              []*pdi.StorageAlert
	maxTxnReportsBeforePause int
	maxTxnBytesBeforePause   int

	txnBatchQueue chan *txnBatch
}




func (session *boltSession) readerWriter() {

    for {
        batch := <- session.txnBatchQueue

        // If nil is sent, it means it's time to exit
        if batch == nil {
            return
        }

        if len(batch.txnsToCommit) > 0 {
            session.commitBatch(batch)
        }

        if len(batch.txnsToReport) > 0 {

        }

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

*/

func (session *boltSession) commitBatch(inBatch *txnBatch) error {

	curTime := plan.Now().UnixSecs

	txns := inBatch.txnsToCommit

    tx, err := session.db.Begin(true)
    if err != nil {
        return err
    }

	err := session.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(storageTxnBucketName)

		for i := range txns {

			txns[i].TimeCommitted = plan.Now().UnixSecs

			// In a db like this, once the entry is written, it won't ever revert
			txns[i].TxnStatus = pdi.TxnStatus_FINALIZED
            
			txnBuf, err := txns[i].Marshal()
			if err != nil {
    			err = plan.Error(err, plan.FailedToCommitTxn, "StorageTxn.Marshal() failed")
			}

			if err == nil {
			    err = bucket.Put(txns[i].TxnName, txnBuf)
            }

			if err != nil {
    			txns[i].TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
                alert := &pdi.StorageAlert{
                    AlertCode: pdi.FailedToCommitTxn,
                    Info: err.Error(),
                    Txn: txns[i],
                    RequestID: inBatch.requestID,
                }
                session.alertOutbox = append(session.alertOutbox, alert)
                log.Println(alert)

                return alert
			}


			if err != nil {
    			txns[i].TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
				return plan.Error(err, plan.FailedToCommitTxn, "error putting txn")
			}

		}

		return nil
	})

	v := err.(pdi.StorageAlert)
    
     {
		txnReportChannel <- TxnReport{
			inBatch.requestID,
			inBatch.txnsToCommit,
		}
	}
}

func (session *boltSession) ReportTxns(
	inTxnNames [][]byte,
	inOmitData bool,
) {

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








type txnReqQueue struct {
	items	[]txnRequest
	head	int
	tail	int
	count	int
}

// Push adds a node to the queue.
func (q *txnReqQueue) Push(inTxnName []byte, bool inOmitData) {
	if q.head == q.tail && q.count > 0 {
		items := make([]txnRequest, len(q.nodes)*2)
		copy(nodes, q.items[q.head:])
		copy(nodes[len(q.items)-q.head:], q.items[:q.head])
		q.head = 0
		q.tail = len(q.items)
		q.items = items
	}
	q.items[q.tail] = txnRequest{inTxnName, inOmitData}
	q.tail = (q.tail + 1) % len(q.items)
	q.count++
}

// Pop removes and returns a node from the queue in first to last order.
func (q *txnReqQueue) Pop() txnRequest {
	if q.count == 0 {
		return nil
	}
	req := q.items[q.head]
	q.head = (q.head + 1) % len(q.items)
	q.count--
	return req
}

*/
