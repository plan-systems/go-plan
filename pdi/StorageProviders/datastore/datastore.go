package datastore

import (

    //"fmt"
    "sync/atomic"
    "os"
    "path"
    "encoding/base64"
    "io/ioutil"
    //"strings"
    "sync"
    "bytes"
    "encoding/json"
    "time"
    "context"
    //"hash"
    //"crypto/rand"

    //"golang.org/x/sync/semaphore"

    log "github.com/sirupsen/logrus"

    //ds "github.com/ipfs/go-datastore"
	//dsq "github.com/ipfs/go-datastore/query"

    "github.com/dgraph-io/badger"

    //"github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"

)


// StorageConfig contains core info about a db/store
type StorageConfig struct {
    HomePath                string                  `json:"home_path"`
    ImplName                string                  `json:"impl_name"`
}


// Store wraps a PLAN community UUID and a datastore
type Store struct {
    CommunityID                 plan.CommunityID
    AgentDesc                   string    
    Config                      *StorageConfig
    Epoch                       pdi.StorageEpoch

    OpState                     OpState

    AbsPath                     string
    keyEncoding                 base64.Encoding

    txnDB                       *badger.DB
    acctDB                      *badger.DB

    // txn status updates.  Intelligently processed such that TxnQuery jobs that are flagged to report new commits receive said new txns
    txnUpdates                   chan txnUpdate

    // Fan-out to committed txn "subscribers"
    subsMutex                   sync.Mutex
    subs                        []chan txnUpdate

    //queryLimit                  *semaphore.Weighted
    //SendJobInbox                chan SendJob
    DecodedCommits              chan CommitJob

    numScanJobs                 int32
    numSendJobs                 int32
    numCommitJobs               int32
 
    TxnDecoder                  pdi.TxnDecoder

    // close when shutting down (polite shutdown)
    //shuttingDown                chan *sync.WaitGroup

    // When OpState == Stopping, shutdown.Wait() is used to block until all queued work is complete.
    resources                   sync.WaitGroup

    // When OpState == Stopping, this context is in a ca
    ctx                         context.Context
    ctxCancel                   context.CancelFunc

    log                         *log.Logger

    DefaultFileMode             os.FileMode

}


type txnUpdate struct {
    UTID            pdi.UTID
    TxnStatus       pdi.TxnStatus
}


// OpState specifices this Store's current operational state. 
type OpState int32
const (

    // Shutdown means this Store is fully shutdown and idle and it is safe to exit this process
    Shutdown OpState = 0

    // Running means this service is running normally
    Running

    // Stopping means this service is currently shutting down
    Stopping
)




// DefaultImplName should be used when a datastore impl is not specified
const DefaultImplName = "badger"

// NewStore makes a new Datastore
func NewStore(
    inConfig *StorageConfig,
    inBasePath string,
) *Store {

    St := &Store{
        Config: inConfig,
        DefaultFileMode: plan.DefaultFileMode,
        log: log.StandardLogger(),
        TxnDecoder: NewTxnDecoder(true),
    }

    if path.IsAbs(St.Config.HomePath) {
        St.AbsPath = St.Config.HomePath
    } else {
        St.AbsPath = path.Clean(path.Join(inBasePath, St.Config.HomePath))
    }

    return St
}

// Startup should be called once Datastore is preprared and ready to invoke the underlying implementation.
func (St *Store) Startup(
    inCtx context.Context,
    inFirstTime bool,
) error {

    if St.OpState != Shutdown {
        return plan.Errorf(nil, plan.AssertFailed, "Store.Startup() cannot start in state %v",  St.OpState)
    }

    logE := log.WithFields(log.Fields{ 
        "impl": St.Config.ImplName,
        "path": St.AbsPath,
    })
    logE.Info( "Startup()" )

    // Load community info
    {
        pathname := path.Join(St.AbsPath, pdi.GenesisEpochFilename)
        buf, err := ioutil.ReadFile(pathname)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "missing %s", pathname)
        }

        err = json.Unmarshal(buf, &St.Epoch)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "error unmarshalling %s", pathname)
        }
    }

    var err error

    switch St.Config.ImplName {
        case "badger":
            opts := badger.DefaultOptions
            opts.Dir = path.Join(St.AbsPath, "txnDB")
            opts.ValueDir = opts.Dir
            St.txnDB, err = badger.Open(opts)
            if err != nil {
                err = plan.Error(err, plan.FailedToLoadDatabase, "txnDB Open() failed")
            } else {
                opts.Dir = path.Join(St.AbsPath, "acctDB")
                opts.ValueDir = opts.Dir
                St.acctDB, err = badger.Open(opts)
                if err != nil {
                    err = plan.Error(err, plan.FailedToLoadDatabase, "acctDB Open() failed")
                    St.txnDB.Close()
                    St.txnDB = nil
                }
            }

        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", St.Config.ImplName)
    }

    if err != nil {
        return err
    }

    St.resources.Add(1)
    St.ctx, St.ctxCancel = context.WithCancel(inCtx)
    //
    //
    //
    //
    // Small buffer needed for the txn notifications to ensure that the txn writer doesn't get blocked
    St.txnUpdates = make(chan txnUpdate, 8)
    go func() {
        for update := range St.txnUpdates {
            St.subsMutex.Lock()
            for _, sub := range St.subs {
                sub <- update
            }
            St.subsMutex.Unlock()
        }
        St.log.Info("1) commit notification exited")

        // Wait until all queries are exited (which is assured with St.OpState set and all possible blocks signaled)
        for {
            time.Sleep(100 * time.Millisecond)
            if atomic.LoadInt32(&St.numScanJobs) == 0 && atomic.LoadInt32(&St.numSendJobs) == 0 {
                break
            }
        }

        St.txnDB.Close()
        St.txnDB = nil

        St.acctDB.Close()
        St.acctDB = nil

        St.log.Info("0) resources released")

        St.OpState = Shutdown
        St.ctx = nil

        St.resources.Done()
    }()
    //
    //
    //
    //
    St.DecodedCommits = make(chan CommitJob, 1)
    go func() {

        for job := range St.DecodedCommits {
            
            // Process once commit job at a time
            St.doCommitJob(job)

            atomic.AddInt32(&St.numCommitJobs, -1)
        }
        St.log.Info("2) commit pipeline closed")

        // Cause all subs to fire, causing them to exit when they see St.OpState == Stopping
        St.txnUpdates <- txnUpdate{}
        close(St.txnUpdates)
    }()


    St.OpState = Running

    go func() {

        select {
            case <- St.ctx.Done():
        }

        St.OpState = Stopping

        // Wait until we're sure a commit didn't sneak in b/c 
        for {
            time.Sleep(100 * time.Millisecond)
            if atomic.LoadInt32(&St.numCommitJobs) == 0 {
                break
            }
        }

        St.log.Info("2) pending commits complete")

        // This will initiate a close-cascade causing St.resources to be released
        close(St.DecodedCommits)
  
    }()


    return nil
}



// Shutdown closes this datastore, if open, blocking until completion.  No-op if this Store is already shutdown.
//
// THREADSAFE
func (St *Store) Shutdown(onComplete *sync.WaitGroup) {

    if St.OpState == Running {
        St.ctxCancel()
    }

    if St.OpState >= Running {
        St.resources.Wait()
        St.log.Print("shutdown complete")
    }

    onComplete.Done()
}




// ScanJob represents a pending Query() call to a StorageProvider
type ScanJob struct {
    TxnScan   *pdi.TxnScan
    Outlet     pdi.StorageProvider_ScanServer
    txnUpdates chan txnUpdate
    OnComplete chan error
}

// SendJob represents a pending SendTxns) calls to a StorageProvider
type SendJob struct {
    UTIDs      [][]byte
    Outlet     pdi.StorageProvider_SendTxnsServer
    OnComplete chan error
}

// CommitJob represents a pending CommitTxn() call to a StorageProvider
type CommitJob struct {
    Txn        pdi.DecodedTxn
    //Stream     pdi.StorageProvider_CommitTxnsServer
    //OnComplete chan error
}





func (St *Store) updateAccount(
    dbTxn *badger.Txn,
    inAcctAddr []byte,
    inOp func(acct *pdi.StorageAccount) error,
) error {

    var (
        creatingNew bool
        acct pdi.StorageAccount
        err error
    )

    // Load the acct from the db
    {
        item, dbErr := dbTxn.Get(inAcctAddr)
        if dbErr == badger.ErrKeyNotFound {
            creatingNew = true
        } else if dbErr == nil {
            err = item.Value(func(val []byte) error {
                return acct.Unmarshal(val)
            })
        }

        if err != nil {
            err = plan.Errorf(err, plan.StorageNotReady, "failed to get/unmarshal account")
        }
    }

    if err == nil {
        err = inOp(&acct)
    }

    // Write the new acct out
    if err == nil {
        var scrap [200]byte
        acctSz, merr := acct.MarshalTo(scrap[:])
        if merr != nil {
            err = plan.Errorf(merr, plan.StorageNotReady, "failed to marshal account")
        }

        if err == nil {
            if creatingNew {
                St.log.Infof("Creating account %v to receive deposit", pdi.Encode64(inAcctAddr))
            }
            err = dbTxn.Set(inAcctAddr, scrap[:acctSz]) 
            if err != nil {
                err = plan.Errorf(err, plan.StorageNotReady, "failed to update acct")
            }
        }
    }

    // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
    if err != nil {
        St.log.WithField( 
            "acct", inAcctAddr,
        ).Warning(err)
    }

    return err
}

// DepositTransfers deposits the given amount to 
func (St *Store) DepositTransfers(inTransfers []*pdi.Transfer) error {

    if len(inTransfers) == 0 {
        return nil
    }

    batch := NewTxnHelper(St.acctDB)

    for batch.NextAttempt() {
        var err error

        for _, xfer := range inTransfers {
            err = St.updateAccount(
                batch.Txn,
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

        batch.Finish(err)
    }

    return batch.FatalErr()
}



func (St *Store) doCommitJob(job CommitJob) error {

    var (
        err error
    ) 


    if err == nil {
        
        batch := NewTxnHelper(St.acctDB)
        for batch.NextAttempt() {

            // Debit the senders account (from Fuel and any transfers ordered)
            {
                fuelForTxn := St.Epoch.FuelPerTxn + int64(St.Epoch.FuelPerKb) * int64( len(job.Txn.RawTxn) >> 10 )

                err = St.updateAccount(
                    batch.Txn,
                    job.Txn.Info.From,
                    func (ioAcct *pdi.StorageAccount) error {

                        // Debit Fuel needed for txn
                        if ioAcct.FuelBalance < fuelForTxn {
                            return plan.Errorf(nil, plan.InsufficientFuel, "insufficient Fuel for txn cost of %v", fuelForTxn)
                        }

                        ioAcct.FuelBalance -= fuelForTxn

                        // Debit explicit transfers
                        for _, xfer := range job.Txn.Info.Transfers {
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
                for _, xfer := range job.Txn.Info.Transfers {
                    err = St.updateAccount(
                        batch.Txn,
                        xfer.To,
                        func (ioAcct *pdi.StorageAccount) error {
                            return ioAcct.Deposit(xfer)
                        },
                    )
                    if err != nil {
                        break
                    }
                }
            }

            batch.Finish(err)
        }

        err = batch.FatalErr()
    }

    // Write the raw txn
    if err == nil {

        batch := NewTxnHelper(St.txnDB)
        for batch.NextAttempt() {

            err := batch.Txn.Set(job.Txn.Info.UTID, job.Txn.RawTxn)
            if err != nil {
                err = plan.Error(err, plan.StorageNotReady, "failed to write raw txn data to db")
            }

            batch.Finish(err)
        }
    }


    {
        txnStatus := pdi.TxnStatus_COMMITTED

        if err != nil {
            txnStatus = pdi.TxnStatus_COMMIT_FAILED

            perr, _ := err.(*plan.Err)
            if perr == nil {
                perr = plan.Error(err, plan.FailedToCommitTxn, "txn commit failed")
            }

            St.log.WithFields(log.Fields{
                "UTID": job.Txn.UTID,
            }).WithError(err).Warn("CommitJob error")
        
            err = perr
        }

        St.txnUpdates <- txnUpdate{
            job.Txn.Info.UTID, 
            txnStatus,
        }
    }

    return err
}


func (St *Store) addTxnSubscriber() chan txnUpdate {
    sub := make(chan txnUpdate, 1)
    
    St.subsMutex.Lock()
    St.subs = append(St.subs, sub)
    St.subsMutex.Unlock()

    return sub
}


func (St *Store) removeTxnSubscriber(inSub chan txnUpdate) {

    St.subsMutex.Lock()
    N := len(St.subs)
    for i := 0; i < N; i++ {
        if St.subs[i] == inSub {
            N--
            St.subs[i] = St.subs[N]
            St.subs[N] = nil
            St.subs = St.subs[:N]
            break
        }
    }
    St.subsMutex.Unlock()

}



// DoScanJob queues the given ScanJob
func (St *Store) DoScanJob(job ScanJob) {

    atomic.AddInt32(&St.numScanJobs, 1)

    err := St.CheckState()
    if err != nil {
        job.OnComplete <- err
        atomic.AddInt32(&St.numScanJobs, -1)
        return
    }

    // TODO: use semaphore.NewWeighted() to bound the number of query jobs
    go func() {
        err := St.doScanJob(job)

        if err != nil {
            St.log.WithError(err).Warn("doQueryJob() returned error")   
        }

        // This releases the GRPC handler
        job.OnComplete <- err

        atomic.AddInt32(&St.numScanJobs, -1)
    }()
}


// DoSendJob queues the given SendJob
func (St *Store) DoSendJob(job SendJob) {
    atomic.AddInt32(&St.numSendJobs, 1)

    err := St.CheckState()
    if err != nil {
        job.OnComplete <- err
        atomic.AddInt32(&St.numSendJobs, -1)
        return
    }

    go func() {
        err := St.doSendJob(job)

        if err != nil {
            St.log.WithError(err).Warn("doSendJob() returned error")   
        }

        // This releases the GRPC handler
        job.OnComplete <- err

        atomic.AddInt32(&St.numSendJobs, -1)
    }()

}


// DoCommitJob queues the given CommitJob
func (St *Store) DoCommitJob(job CommitJob) error {
    atomic.AddInt32(&St.numCommitJobs, 1)

    err := St.CheckState()
    if err == nil {
        err = job.Txn.DecodeRawTxn(St.TxnDecoder)
    }

    if err != nil {
        atomic.AddInt32(&St.numSendJobs, -1)
        return err
    }

    St.txnUpdates <- txnUpdate{
        job.Txn.Info.UTID, 
        pdi.TxnStatus_COMMITTING,
    }

    St.DecodedCommits <- job

    return nil
}

// CheckState returns an error if this Store is shutting down (or otherwise not in a state to continue service)
func (St *Store) CheckState() error {

    if St.OpState < Running {
        return plan.Errorf(nil, plan.ServiceShutdown, "service is shutdown")
    } else if St.OpState > Running  {
        return plan.Errorf(nil, plan.ServiceShutdown, "service shutting down")
    }

    return nil
}



func (St *Store) doSendJob(job SendJob) error {

    var err error

    dbTxn := St.txnDB.NewTransaction(false)
    {
        txn := pdi.Txn{
            TxnMetaInfo: &pdi.TxnMetaInfo{
                TxnStatus: pdi.TxnStatus_FINALIZED,
            },
        }

        for _, UTID := range job.UTIDs {
            var txnOut *pdi.Txn

            if err == nil {
                item, dbErr := dbTxn.Get(UTID)
                if dbErr == nil {
                    txnOut = &txn

                    err = item.Value(func(inVal []byte) error{
                        txnOut.RawTxn = inVal
                        return nil
                    })
                } else if dbErr == badger.ErrKeyNotFound  {
                    // TODO: send alert that UTID not found?
                } else {
                    err = dbErr
                }

                if err != nil {
                    err = plan.Errorf(err, plan.TxnDBNotReady, "failed to read txn")
                    St.log.WithError(err).Warn("txnDB error")
                    continue
                }
            }

            err = St.CheckState()
            if err != nil && txnOut != nil {
                err = job.Outlet.Send(txnOut)
            }

            if err != nil {
                break
            }
        }
    }
    dbTxn.Discard()
    dbTxn = nil

    return err

}



func (St *Store) doScanJob(job ScanJob) error {

    var err error

    const (
        batchMax = 20
        batchBufSz = batchMax * pdi.UTIDBinarySz
    )
    var (
        UTIDs [batchMax][]byte
    )
    statuses := make([]byte, batchMax)
    for i := 0; i < batchMax; i++ {
        statuses[i] = byte(pdi.TxnStatus_FINALIZED)
    }

    // Before we start the db txn (and effectively get locked to a db rev in time), subscribe this job to receive new commits
    if job.TxnScan.SendTxnUpdates {
        job.txnUpdates = St.addTxnSubscriber()
    }

    dbTxn := St.txnDB.NewTransaction(false)
    {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        opts.Reverse = job.TxnScan.TimestampStart > job.TxnScan.TimestampStop
        itr := dbTxn.NewIterator(opts)

        var (
            dir int
            keyBuf [pdi.UTIDBinarySz]byte
        )

        itr.Seek(  pdi.UTIDFromInfo(keyBuf[:0], job.TxnScan.TimestampStart, nil))
        stopKey := pdi.UTIDFromInfo(keyBuf[:0], job.TxnScan.TimestampStop,  nil)

        if opts.Reverse {
            dir = -1
        } else {
            dir = 1
        }

        totalCount := int32(0)

        for dir != 0 && err == nil {
            batchCount := int32(0)
            
            for  ; itr.Valid(); itr.Next() {
                err = St.CheckState()
                if err != nil {
                    break
                }

                item := itr.Item()
                itemKey := item.Key()
                if dir * bytes.Compare(itemKey, stopKey) > 0 {
                    dir = 0
                    break
                }

                if len(itemKey) != pdi.UTIDBinarySz {
                    St.log.Warnf("encountered txn key len %d, expected %d", len(itemKey), pdi.UTIDBinarySz)
                    continue
                }

                copy(UTIDs[batchCount], itemKey)

                batchCount++
                totalCount++
                if totalCount == job.TxnScan.MaxTxns {
                    dir = 0
                    break
                } else if batchCount == batchMax {
                    break
                }
            }

            if err == nil && batchCount > 0 {
                err = job.Outlet.Send(&pdi.TxnList{
                    UTIDs:    UTIDs[:batchCount],
                    Statuses: statuses[:batchCount],
                })
            }
        }

        itr.Close()
    }

    dbTxn.Discard()
    dbTxn = nil

    if job.TxnScan.SendTxnUpdates {
        heartbeat := time.NewTicker(time.Second * 10)
        batchLag  := time.NewTicker(time.Millisecond * 300)

        batchCount := int32(0)
        wakeTimer := heartbeat.C

        for err == nil && St.CheckState() != nil {

            select {

                case txnUpdate := <- job.txnUpdates:
                    if len(txnUpdate.UTID) != pdi.UTIDBinarySz {
                        copy(UTIDs[batchCount], txnUpdate.UTID)
                        statuses[batchCount] = byte(txnUpdate.TxnStatus)
                        batchCount++

                        if batchCount == 1 {
                            wakeTimer = batchLag.C
                            for len(wakeTimer) > 0 {
                                <-wakeTimer
                            }
                        }
                    }
                        
                case <- wakeTimer:
                    err = job.Outlet.Send(&pdi.TxnList{
                        UTIDs: UTIDs[:batchCount],
                        Statuses: statuses[:batchCount],
                    })
                    wakeTimer = heartbeat.C
            }
        }

        St.removeTxnSubscriber(job.txnUpdates)
    }

    return err
}
