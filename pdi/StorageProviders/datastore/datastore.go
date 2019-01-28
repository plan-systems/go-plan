package datastore

import (

    //"fmt"
    "os"
    "path"
    "encoding/base64"
    //"io/ioutil"
    //"strings"
    "sync"
    //"time"
    //"hash"
    //"crypto/rand"



    log "github.com/sirupsen/logrus"

    ds "github.com/ipfs/go-datastore"
    badger "github.com/ipfs/go-ds-badger"

    //"github.com/tidwall/redcon"

    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"
    //_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

    "github.com/ethereum/go-ethereum/common/hexutil"

  
)



// StorageInfo contains core info about a db/store
type StorageInfo struct {
    CommunityName           string                  `json:"community_name"`
    CommunityID             hexutil.Bytes           `json:"community_id"`
    HomePath                string                  `json:"home_path"`
    TimeCreated             plan.Time               `json:"time_created"`  
    ImplName                string                  `json:"impl_name"`
    Params                  map[string]string       `json:"params"`
}


/*
func SupportsStorageType(inParams map[string]string) *plan.Perror {

}*/

type failedJob struct {
    Error                       *plan.Perror
    QueryJob                    *QueryJob
    CommitJob                   *CommitJob
}

// Store wraps a PLAN community UUID and a datastore
type Store struct {
    CommunityID                 plan.CommunityID
    AgentDesc                   string    
    Info                        *StorageInfo

    AbsPath                     string
    keyEncoding                 base64.Encoding

    ds                          ds.TxnDatastore
    closeDs                     func()

    QueryInbox                  chan *QueryJob
    CommitInbox                 chan *CommitJob

    FailedJobs                  chan failedJob    

    // close when shutting down (polite shutdown)
    shuttingDown                chan *sync.WaitGroup

    Agent                       pdi.StorageProviderAgent

    //throttleCommits             bool

    //msgOutbox                   chan *pservice.Msg
    //msgInbox                    chan *pservice.Msg

    DefaultFileMode             os.FileMode

}


// NewStore makes a new Datastore
func NewStore(
    inInfo *StorageInfo,
    inBasePath string,
) *Store {

    St := &Store{
        Info: inInfo,
        DefaultFileMode: plan.DefaultFileMode,
        QueryInbox: make(chan *QueryJob),
        CommitInbox: make(chan *CommitJob, 16),
        FailedJobs: make(chan failedJob, 4),
        shuttingDown: make(chan *sync.WaitGroup),
    }

    St.Agent, _ = NewAgent("", 0)

    if path.IsAbs(St.Info.HomePath) {
        St.AbsPath = St.Info.HomePath
    } else {
        St.AbsPath = path.Clean(path.Join(inBasePath, St.Info.HomePath))
    }

    return St
}

// OnServiceStarting should be called once Datastore is preprared and ready to invoke the underlying implementation.
func (St *Store) OnServiceStarting() error {

    logE := log.WithFields(log.Fields{ 
        "impl": St.Info.ImplName,
        "path": St.AbsPath,
    })
    logE.Info( "OnServiceStarting()" )

    var err error

    switch St.Info.ImplName {
        case "badger":
            badgerDS, err := badger.NewDatastore(St.AbsPath, nil)
            if err == nil {
                St.ds = badgerDS
                St.closeDs = func() {
                    badgerDS.Close()
                }
            }
        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", St.Info.ImplName)
    }

    if err == nil {
        St.startProcessingJobs()
    } else {
        logE := logE.WithError(err)
        logE.Warn("OnServiceStarting() ERROR")
    }


    return err
}


// Shutdown closes this datastore, if open, blocking until completion.
func (St *Store) Shutdown(inGroup *sync.WaitGroup) {

    // Signal a shutdown
    var waiter sync.WaitGroup
    waiter.Add(2)
    St.shuttingDown <- &waiter
    St.shuttingDown <- &waiter
    waiter.Wait()

    if St.closeDs != nil {
        St.closeDs()
        St.closeDs = nil
    }

    inGroup.Done()
}



// QueryJob represents a pending Query() call to a StorageProvider
type QueryJob struct {
    QueryTxns *pdi.QueryTxns
    Outlet     pdi.StorageProvider_QueryServer
    OnComplete chan *plan.Perror
}

// CommitJob represents a pending CommitTxn() call to a StorageProvider
type CommitJob struct {
    RawTxn    *pdi.RawTxn
    Outlet     pdi.StorageProvider_CommitTxnServer
    OnComplete chan *plan.Perror
}



// UTIDKeyForTxn creates a ds.Key based on the transaction's timestamp and hash.UTIDKeyForTxn
// See comments for TxnInfo.UTID 
func UTIDKeyForTxn(txnInfo *pdi.TxnInfo) ds.Key {
    key := pdi.ConvertToUTID("/", txnInfo.TimeSealed, txnInfo.TxnDigest)
    return ds.RawKey(key)
}


/*
func (k ds.Key) Equals(inTime int64, inTxnName []byte) bool {
    if len(k) != encodedKeyLen {
        return false
    }
    if binary.BigEndian.Uint64(tk[0:8]) != uint64(inTime) {
        return false
    }
    if bytes.Compare(tk[8:16], inTxnName) != 0 {
        return false
    }

    return true
}
*/


func (St *Store) startProcessingJobs() {

    // CommitInbox processor
    go func() {

        var doneSignal *sync.WaitGroup

        // TODO: add throttling?

        // Don't' stop looping until the commit inbox is clear
        for doneSignal == nil {

            select {
                case commitJob := <- St.CommitInbox:
                    St.doCommitJob(commitJob)

                case doneSignal = <-St.shuttingDown:
            }
        }

        doneSignal.Done()
    }()

    // QueryInbox processor
    go func() {

        var doneSignal *sync.WaitGroup

        for doneSignal == nil {

            select {
                case queryJob := <- St.QueryInbox:
                    St.doQueryJob(queryJob)

                case doneSignal = <-St.shuttingDown:
            }
        }

        doneSignal.Done()
    }()

}

var txnCommitting = pdi.TxnMetaInfo{
    TxnStatus: pdi.TxnStatus_COMMITTING,
}
var txnFinalized = pdi.TxnMetaInfo{
    TxnStatus: pdi.TxnStatus_FINALIZED,
}

func (St *Store) doCommitJob(commitJob *CommitJob) {

    commitJob.Outlet.Send(&txnCommitting)

    var txnInfo pdi.TxnInfo

    err := St.Agent.DecodeRawTxn(
        commitJob.RawTxn.TxnData,
        &txnInfo,
        nil,
    )

    if err == nil {
        key := UTIDKeyForTxn(&txnInfo)

        putErr := St.ds.Put(key, commitJob.RawTxn.TxnData)
        if putErr != nil {
            err = plan.Error(putErr, plan.FailedToCommitTxn, "datastore.Put() failed")
        }
    }

    if err == nil {

        // Since this is a centralized db, every txn committed is finalized
        commitJob.Outlet.Send(&txnFinalized)

    } else {

        commitJob.Outlet.Send(&pdi.TxnMetaInfo{
            TxnStatus: pdi.TxnStatus_FAILED_TO_COMMIT,
            Alert: &plan.Status{
                Code: err.Code,
                Msg: err.Msg,
            },
        })

        fj := failedJob{
            Error: err,
            CommitJob: commitJob,
        }
        St.FailedJobs <- fj
    }

    // This releases the GRPC handler
    commitJob.OnComplete <- err
}


func (St *Store) doQueryJob(queryJob *QueryJob) {

    var perr *plan.Perror
    if perr != nil {

        fj := failedJob{
            Error: perr,
            QueryJob: queryJob,
        }
        St.FailedJobs <- fj
    }

    // This releases the GRPC handler
    queryJob.OnComplete <- perr


}


/*
var txnPool = sync.Pool{
    New: func() interface{} {
        return new(pdi.Txn)
    },
}
*/
/*
func (k *ds.Key) formKeyFromTxn()

func (St *Store) writer() {


}
*/

/*
func (St *Store) readerWriter() {

    S.txnPool = sync.Pool{
        New: func() interface{} {
            return new(pdi.Txn)
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

        // FIRST, choose requests made via RequestTxns() and CommitTxns(), THEN make incremental progress from the read head.
        select {
            case queryJob := <- S.QueryInbox:

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

*/