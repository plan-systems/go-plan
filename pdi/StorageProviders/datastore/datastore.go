package datastore

import (

    //"fmt"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    //"sync"
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

// Store wraps a PLAN community UUID and a datastore
type Store struct {
    CommunityID                 plan.CommunityID
    AgentStr                    string    
    Info                        *StorageInfo

    AbsPath                     string

    ds                          ds.TxnDatastore
    closeDs                     func()

    QueryInbox                  chan *QueryJob
    CommitInbox                 chan *CommitJob

    //msgOutbox                   chan *pservice.Msg
    //msgInbox                    chan *pservice.Msg

    DefaultFileMode             os.FileMode

}


// NewStore makes a new Datastore
func NewStore(
    inInfo *StorageInfo,
    inBasePath string,
) *Store {

    S := &Store{
        Info: inInfo,
        DefaultFileMode: plan.DefaultFileMode,
        QueryInbox: make(chan *QueryJob),
        CommitInbox: make(chan *CommitJob),
    }

    if path.IsAbs(S.Info.HomePath) {
        S.AbsPath = S.Info.HomePath
    } else {
        S.AbsPath = path.Clean(path.Join(inBasePath, S.Info.HomePath, "yoyo"))
    }

    return S
}


// OnServiceStarting should be once Datastore is preprared and ready to invoke the underlying implementation.
func (S *Store) OnServiceStarting() error {

    logE := log.WithFields(log.Fields{ 
        "impl": S.Info.ImplName,
        "path": S.AbsPath,
    })
    logE.Info( "OnServiceStarting()" )

    var err error

    switch S.Info.ImplName {
        case "badger":
            badgerDS, err := badger.NewDatastore(S.AbsPath, nil)
            if err == nil {
                S.ds = badgerDS
                S.AgentStr = "/datastore/badger:1"
                S.closeDs = func() {
                    badgerDS.Close()
                }
            }
        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", S.Info.ImplName)
    }

    if err != nil {
        logE := logE.WithError(err)
        logE.Warn("OnServiceStarting() ERROR")
    }

    return err
}


// Close closes this datastore, if open, blocking until completion.
func (S *Store) Close() {

    if S.closeDs != nil {
        S.closeDs()
        S.closeDs = nil
    }
}

// QueryJob represents a pending Query() call to a StorageProvider
type QueryJob struct {
    QueryTxns *pdi.QueryTxns
    Outlet     pdi.StorageProvider_QueryServer
    OnComplete chan error
}

// CommitJob represents a pending CommitTxn() call to a StorageProvider
type CommitJob struct {
    RawTxn    *pdi.RawTxn
    Outlet     pdi.StorageProvider_CommitTxnServer
    OnComplete chan error
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

func (S *Store) writer() {

    for {
        
        select {
            case commitJob := S.CommitInbox:
                S.ds.Has(

    }

}
*/

/*
func (S *Store) readerWriter() {

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