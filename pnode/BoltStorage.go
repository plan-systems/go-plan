
package pnode

// See: go-plan/pdi/StorageProvider.go for interface context


import (
    "encoding/base64"
    "path"
    "os"
    "time"
    

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
    sessions            []*boltSession
    localPathname       string
    alertHandler        pdi.StorageAlertHandler
    fileMode            os.FileMode
}
 
// NewBoltStorage creates a bolt-based StorageProvider with the given params
func NewBoltStorage(
    inBasePathname string,
    inFileMode os.FileMode,
    ) *BoltStorage {

    storage := &BoltStorage{
        nil,
        inBasePathname,
        nil,
        inFileMode,
    }

    return storage
}





// StartSession implements StorageProvider.StartSession
func (storage *BoltStorage) StartSession(
    inDatabaseID []byte,
    inOnCompletion func(pdi.StorageSession, error),
    inOnTxnReport func(inTxns []*pdi.StorageTxn),
    inOnSessionEnded func(inReason string),
) error {


    if len(inDatabaseID) < 2 || len(inDatabaseID) > 64 {
        return plan.Errorf(nil, plan.InvalidDatabaseID, "got database ID of length %d", len(inDatabaseID))
    }

    var dbName []byte 
    base64.RawURLEncoding.Encode(dbName, inDatabaseID)

    session := &boltSession {
        parentProvider: storage,
        onTxnReport: inOnTxnReport,
        onSessionEnded: inOnSessionEnded,
        dbPathname: path.Join(storage.localPathname, string(dbName) + ".bolt"),
    }

    opt := bolt.Options {
        Timeout:    2 * time.Second,
        NoGrowSync: false,
    }

    var err error
    session.db, err = bolt.Open(session.dbPathname, storage.fileMode, &opt)
    if err != nil {
        return plan.Errorf(err, plan.FailedToLoadDatabase, "failed to open bolt db %s", session.dbPathname)
    }

    session.db.Update( func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists(storageTxnBucketName)

        return err
    })
    if err != nil {
        return plan.Error(err, plan.FailedToLoadDatabase, "failed to create root bucket")
    }

    storage.sessions = append(storage.sessions, session)

    inOnCompletion(session, nil)

    return nil
}

// SegmentIntoTxnsForCommit implements StorageProvider.SegmentIntoTxnsForCommit
func (storage *BoltStorage) SegmentIntoTxnsForCommit(
    inData []byte, 
    inDataDesc pdi.TxnDataDesc,
) ([]*pdi.StorageTxn, error) {

    return pdi.SegmentIntoTxnsForMaxSize(inData, inDataDesc, 32000)
}




func (storage *BoltStorage) endSession(inSession *boltSession, inReason string) *plan.Perror {
    for i, session := range storage.sessions {
        if session == inSession {
            n := len(storage.sessions)-1
            storage.sessions[i] = storage.sessions[n]
            storage.sessions = storage.sessions[:n]
            if inSession.onSessionEnded != nil {
                inSession.onSessionEnded(inReason)
            }
            return nil
        }
    }

    return plan.Error(nil, plan.InvalidStorageSession, "storage session not found")
}





type boltSession struct {
    parentProvider *BoltStorage
    onTxnReport func(inTxns []*pdi.StorageTxn)
    onSessionEnded func(inReason string)

    dbPathname string
    db *bolt.DB
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



/*
     for i, txnName := range inTxnNames {
            bucketGet(key []byte) []byte

        for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
            fmt.Printf("key=%s, value=%s\n", k, v)
        }

        b := tx.Bucket([]byte("MyBucket"))
        v := b.Get([]byte("answer"))

    })*/


func (session *boltSession) StartReporting(
    inStartFromTime int64,
    ) error {

    return nil;
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

    session.parentProvider.endSession(session, inReason)
}


