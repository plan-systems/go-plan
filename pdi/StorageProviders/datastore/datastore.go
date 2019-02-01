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

    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"
    //_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"


  
)


// StorageConfig contains core info about a db/store
type StorageConfig struct {
    HomePath                string                  `json:"home_path"`
    ImplName                string                  `json:"impl_name"`
    Epoch                   plan.CommunityEpoch          `json:"epoch"`
}




type failedJob struct {
    Error                       error
    QueryJob                    *QueryJob
    CommitJob                   *CommitJob
}

// Store wraps a PLAN community UUID and a datastore
type Store struct {
    CommunityID                 plan.CommunityID
    AgentDesc                   string    
    Config                      *StorageConfig

    AbsPath                     string
    keyEncoding                 base64.Encoding

    commitScrap                 []byte

    ds                          ds.TxnDatastore
    closeDs                     func()

    QueryInbox                  chan *QueryJob
    CommitInbox                 chan *CommitJob

    FailedJobs                  chan failedJob    

    // close when shutting down (polite shutdown)
    shuttingDown                chan *sync.WaitGroup

    TxnDecoder                   pdi.TxnDecoder
    //stDecoder                   pdi.TxnDecoder

    log                         *log.Logger

    //throttleCommits             bool

    //msgOutbox                   chan *pservice.Msg
    //msgInbox                    chan *pservice.Msg

    DefaultFileMode             os.FileMode

}


/*
type Log struct {
}



func (L *Log) LogErr(inErr Err) {
    log.Error(inErr)
}

func (L *Log) LogErr(inErr Err) {
    log.Error(inErr)
}
*/



// NewStore makes a new Datastore
func NewStore(
    inConfig *StorageConfig,
    inBasePath string,
) *Store {

    St := &Store{
        Config: inConfig,
        DefaultFileMode: plan.DefaultFileMode,
        QueryInbox: make(chan *QueryJob),
        CommitInbox: make(chan *CommitJob, 16),
        FailedJobs: make(chan failedJob, 4),
        shuttingDown: make(chan *sync.WaitGroup),
        log: log.StandardLogger(),
        //commitScrap: make([]byte, 4000),
    }

    St.TxnDecoder = NewTxnDecoder()

    if path.IsAbs(St.Config.HomePath) {
        St.AbsPath = St.Config.HomePath
    } else {
        St.AbsPath = path.Clean(path.Join(inBasePath, St.Config.HomePath))
    }

    return St
}

// Startup should be called once Datastore is preprared and ready to invoke the underlying implementation.
func (St *Store) Startup(inFirstTime bool) error {

    logE := log.WithFields(log.Fields{ 
        "impl": St.Config.ImplName,
        "path": St.AbsPath,
    })
    logE.Info( "Startup()" )

    var err error

    switch St.Config.ImplName {
        case "badger":
            badgerDS, berr := badger.NewDatastore(St.AbsPath, nil)
            if berr != nil {
                err = plan.Error(berr, plan.FailedToLoadDatabase, "badger.NewDatastore() failed")
            } else {
                St.ds = badgerDS
                St.closeDs = func() {
                    badgerDS.Close()
                }
            }
        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", St.Config.ImplName)
    }

    if err != nil {
        return err
    }

    St.startProcessingJobs()

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
    OnComplete chan error
}

// CommitJob represents a pending CommitTxn() call to a StorageProvider
type CommitJob struct {
    RawTxn    *pdi.RawTxn
    Outlet     pdi.StorageProvider_CommitTxnServer
    OnComplete chan error
}



// UTIDKeyForTxn creates a ds.Key based on the transaction's timestamp and hash.UTIDKeyForTxn
// See comments for ConvertToUTID
func UTIDKeyForTxn(txnInfo *pdi.TxnInfo) ds.Key {
    key := pdi.ConvertToUTID("/", txnInfo.TimeSealed, txnInfo.TxnHashname)
    return ds.RawKey(key)
}






// AccountKeyForPubKey makes a datastore Key for a given PubKey
func AccountKeyForPubKey(inPubKey *ski.PubKey) ds.Key {

    const (
        maxPubKeyLen = 24       // Arbitrary. As a ref point, BTC and ETH public address size is 20 bytes
        encodedKeyLen = (maxPubKeyLen * 8 + 5) / 6
        prefixLen = 3
    )

    pubKey := inPubKey.Base256()
	overhang := len(pubKey) - maxPubKeyLen
    if overhang > 0 {
        pubKey = pubKey[overhang:]
    }

    var out [encodedKeyLen+prefixLen]byte
    out[0] = '/'
    out[1] = '@'
    out[2] = '/'
	pdi.Base64.Encode(out[prefixLen:], pubKey)

    finalLen := prefixLen + pdi.Base64.EncodedLen(len(pubKey))
    return ds.RawKey(string(out[:finalLen]))
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


/*
func (St *Store) loadAccount(inKey ds.Key, outAcct *pdi.StorageAccount) {

}
*/

type accountVerb int

const (
    withdraw accountVerb = iota
    deposit
)



func (St *Store) updateAccount(
    dsTxn ds.Datastore,
    inAcct *ski.PubKey,
    inOp func(acct *pdi.StorageAccount) error,
) (ds.Key, error) {

    dsKey := AccountKeyForPubKey(inAcct)
    var (
        creatingNew bool
        acct pdi.StorageAccount
        err error
    )

    // Load the acct from the db
    {
        var val []byte
        val, err = dsTxn.Get(dsKey)
        if err == ds.ErrNotFound {
            err = nil
            creatingNew = true
        } else if err == nil {
            err = acct.Unmarshal(val)
        }

        if err != nil {
            err = plan.Errorf(err, plan.StorageNotReady, "failed to get/unmarshal account %v", dsKey)
        }
    }

    if err == nil {
        err = inOp(&acct)
    }

    // Write the new acct out
    if err == nil {
        var scrap [200]byte
        acctSz, err := acct.MarshalTo(scrap[:])
        if err != nil {
            err = plan.Errorf(err, plan.StorageNotReady, "failed to marshal account %v", dsKey)
        }

        if err == nil {
            if creatingNew {
                St.log.Infof("Creating account %v to receive deposit", dsKey)
            }
            err = dsTxn.Put(dsKey, scrap[:acctSz]) 
            if err != nil {
                err = plan.Errorf(err, plan.StorageNotReady, "failed to update acct %v", dsKey)
            }
        }
    }

    // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
    if err != nil {
        St.log.WithField( 
            "dsKey", dsKey,
        ).Warning(err)
    }

    return dsKey, err
}


/*
func (St *Store) depositTransfers(
    dsTxn ds.Txn,
    inTransfers []*pdi.Transfer) {

    
    for _, xfer := range inTransfers {
        dsKey := AccountKeyForPubKey(xfer.To)
        
        var acct pdi.StorageAccount

        val, err := dsTxn.Get(dsKey)
        if err == ds.ErrNotFound {
            St.log.Infof("Creating account %v to receive deposit", dsKey)
            err = nil
        } else if err == nil {
            if 2 * len(val) < cap(St.commitScrap) {
                St.commitScrap = make([]byte, 2 * len(val) + 1000)
            }

            err = acct.Unmarshal(val)
        }

        if err == nil {
            err = acct.Deposit(xfer)
        }

        // Write the new acct out
        if err == nil {
            var n int
            n, err = acct.MarshalTo(St.commitScrap)
            if err == nil {
                err = dsTxn.Put(dsKey, St.commitScrap[:n]) 
            }
        }

        // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
        if err != nil {
            St.log.WithField( 
                "dsKey", dsKey,
            ).Warning(err)
        }
    }

}
*/

// TODO: make txn iterator so that failed Commits() are retried 2-3 times, etc
func (St *Store) newTxn(readOnly bool) (ds.Txn, error) {

    dsTxn, err := St.ds.NewTransaction(readOnly)
    if err != nil {
        return nil, plan.Errorf(err, plan.StorageNotReady, "St.ds.NewTransaction() failed")
    }

    return dsTxn, nil
}




// DepositTransfers deposits the given amount to 
func (St *Store) DepositTransfers(inTransfers []*pdi.Transfer) error {

    if len(inTransfers) == 0 {
        return nil
    }

    txn := St.NewTxnHelper()

    for txn.NextAttempt() {
        var err error

        for _, xfer := range inTransfers {
            _, err = St.updateAccount(
                txn.dsTxn,
                xfer.To,
                func (ioAcct *pdi.StorageAccount) error {
                    return ioAcct.Deposit(xfer) 
                },
            )

            // Bail if any deposit returns an err
            if err != nil {
                break
            }
        }

        txn.Finish(err)
    }

    return txn.FatalErr()
}


func (St *Store) doCommitJob(commitJob *CommitJob) {

    commitJob.Outlet.Send(&txnCommitting)

    var (
        txnInfo pdi.TxnInfo
        dsKey ds.Key
    )


    err := St.TxnDecoder.DecodeRawTxn(
        commitJob.RawTxn.TxnData,
        &txnInfo,
        nil,
    )

    if err == nil {
        txn := St.NewTxnHelper()

        for txn.NextAttempt() {

            // Debit the senders account (from gas and any transfers ordered)
            {
                gasForTxn := St.Config.Epoch.GasTxnBase + int64(St.Config.Epoch.GasPerKb) * int64( len(commitJob.RawTxn.TxnData) >> 10 )

                dsKey, err = St.updateAccount(
                    txn.dsTxn,
                    txnInfo.From,
                    func (ioAcct *pdi.StorageAccount) error {

                        // Debit gas needed for txn
                        if ioAcct.GasBalance < gasForTxn {
                            return plan.Error(nil, plan.InsufficientGas, "txn sender has insufficient gas")
                        }

                        ioAcct.GasBalance -= gasForTxn

                        // Debit explicit transfers
                        for _, xfer := range txnInfo.Transfers {
                            err = ioAcct.Withdraw(xfer)
                            if err != nil {
                                return err
                            }
                        }

                        return nil
                    },
                )
            }

            // Deposit txn credits to recipients
            if err == nil {
                for _, xfer := range txnInfo.Transfers {
                    _, err = St.updateAccount(
                        txn.dsTxn,
                        xfer.To,
                        func (ioAcct *pdi.StorageAccount) error {
                            return ioAcct.Deposit(xfer)
                        },
                    )
                }
            }

            // Write the raw txn
            if err == nil {
                key := UTIDKeyForTxn(&txnInfo)
                err := txn.dsTxn.Put(key, commitJob.RawTxn.TxnData)
                if err != nil {
                    err = plan.Error(err, plan.StorageNotReady, "failed to write raw txn data to db")
                }
            }

            txn.Finish(err)
        }

        err = txn.FatalErr()
    }


    if err == nil {

        // Since this is a centralized db, every txn committed is finalized
        commitJob.Outlet.Send(&txnFinalized)

    } else {

        perr, _ := err.(*plan.Err)
        if perr == nil {
            perr = plan.Error(err, plan.FailedToCommitTxn, "txn commit failed")
        }

        St.log.WithFields(log.Fields{
            "dsKey": dsKey,
        }).Error(err)

        commitJob.Outlet.Send(&pdi.TxnMetaInfo{
            TxnStatus: pdi.TxnStatus_FAILED_TO_COMMIT,
            Alert: &plan.Status{
                Code: perr.Code,
                Msg: perr.Msg,
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

    var err error
    if err != nil {

        fj := failedJob{
            Error: err,
            QueryJob: queryJob,
        }
        St.FailedJobs <- fj
    }

    // This releases the GRPC handler
    queryJob.OnComplete <- err


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