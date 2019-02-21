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

    // UTIDs newly committed.  Intelligently processed such that TxnQuery jobs that are flagged to report new commits receive said new txns
    newTxns                     chan pdi.UTID

    // Fan-out to committed txn "subscribers"
    subsMutex                   sync.Mutex
    subs                        []chan pdi.UTID

    //queryLimit                  *semaphore.Weighted
    //SendJobInbox                chan SendJob
    CommitInbox                 chan CommitJob
    decodedCommits              chan CommitJob

    numQueryJobs                int32
    numSendJobs                 int32
 
    TxnDecoder                  pdi.TxnDecoder

    // close when shutting down (polite shutdown)
    //shuttingDown                chan *sync.WaitGroup

    // When OpState == Stopping, shutdown.Wait() is used to block until all queued work is complete.
    level1                      sync.WaitGroup
    level0                      sync.WaitGroup

    // When OpState == Stopping, this context is in a ca
    ctx                         context.Context
    ctxCancel                   context.CancelFunc

    log                         *log.Logger

    DefaultFileMode             os.FileMode

}


// OpState specifices this Store's current operatational state. 
type OpState int
const (

    // Stopped means this Store is fully shutdown and idle and it is safe to exit this process
    Stopped OpState = 0

    // Starting means this Store is starting up, doi
    Starting

    // Running means this service is running normally
    Running

    // Stopping means this service is currently shuttiing down
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

    if St.OpState != Stopped {
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
                }
            }

        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", St.Config.ImplName)
    }

    if err != nil {
        return err
    }

    St.OpState = Starting

    St.level0.Add(1)
    St.ctx, St.ctxCancel = context.WithCancel(inCtx)


    // Small buffer needed for the txn notifications to ensure that the txn writer doesn't get blocked
    St.newTxns = make(chan pdi.UTID, 2)
    St.level1.Add(1)
    go func() {
        for newTxn := range St.newTxns {
            St.subsMutex.Lock()
            for _, sub := range St.subs {
                sub <- newTxn
            }
            St.subsMutex.Unlock()
        }

        St.level1.Done()
    }()


    St.decodedCommits = make(chan CommitJob, 1)
    go func() {

        for job := range St.decodedCommits {
            
            // Unlike QueryJobs, we only process once commit job at a time
            St.doCommitJob(job)
        }

        close(St.newTxns)
    }()


    // No buffer needed for the entry point channel and would serve no purpose.
    St.CommitInbox = make(chan CommitJob)
    go func() {

        for job := range St.CommitInbox {
            
            err := job.Txn.DecodeRawTxn(St.TxnDecoder)
            if err == nil {
                err = job.Outlet.Send(&pdi.TxnMetaInfo{
                    TxnStatus: pdi.TxnStatus_AWAITING_COMMIT,
                })
            }

            if err == nil {
                St.decodedCommits <- job
            }
        }

        close(St.decodedCommits)
    }()


    St.OpState = Running

    go func() {

        select {
            case <- St.ctx.Done():
        }

        St.OpState = Stopping

        // This will initiate a close cascade causing level1 to be released
        close(St.CommitInbox)

        // Wait until all the queries are donzo
        for atomic.LoadInt32(&St.numQueryJobs) > 0 || atomic.LoadInt32(&St.numSendJobs) > 0 {
            time.Sleep(100 * time.Millisecond)
        }

        St.level1.Wait()

        St.txnDB.Close()
        St.txnDB = nil

        St.acctDB.Close()
        St.acctDB = nil

        St.OpState = Stopped

        St.level0.Done()
    }()


    return nil
}



// Shutdown closes this datastore, if open, blocking until completion.
func (St *Store) Shutdown(inGroup *sync.WaitGroup) {

    if St.OpState == Running {
        St.ctxCancel()
    }

    St.level0.Wait()

   if inGroup != nil {
       inGroup.Done()
   }
}


// RetryCriticalOp is called when the given error has occurred and represents a critical block from providing core services.  
//    (e.g. db error while accessing the main txn DB).
//
// No op and returns false if inErr == nil.
//
// Returns true if the caller should retry the given operation, otherwise this CR will transition into a protected/halted state.
func (St *Store) RetryCriticalOp(inErr error) bool {

    if inErr == nil {
        return false
    }

    // TODO: log to disk, etc
    St.log.WithError(inErr).Warnf("critical op failed")

    return false
}




// QueryJob represents a pending Query() call to a StorageProvider
type QueryJob struct {
    TxnQuery   *pdi.TxnQuery
    Outlet     pdi.StorageProvider_QueryServer
    NewTxns    chan pdi.UTID
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
    Outlet     pdi.StorageProvider_CommitTxnServer
    OnComplete chan error
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



func (St *Store) doCommitJob(job CommitJob) {

    var (
        err error
        txnMetaInfo pdi.TxnMetaInfo
    ) 

    if err == nil {
        
        txnMetaInfo.TxnStatus = pdi.TxnStatus_COMMITTING
        job.Outlet.Send(&txnMetaInfo)

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

    if err == nil {

        // Since this is a centralized db, every txn committed is finalized
        txnMetaInfo.TxnStatus = pdi.TxnStatus_FINALIZED
        job.Outlet.Send(&txnMetaInfo)

        St.newTxns <- job.Txn.Info.UTID

    } else {

        perr, _ := err.(*plan.Err)
        if perr == nil {
            perr = plan.Error(err, plan.FailedToCommitTxn, "txn commit failed")
        }

        St.log.WithFields(log.Fields{
            "UTID": job.Txn.UTID,
        }).Warn(err)

        txnMetaInfo.TxnStatus = pdi.TxnStatus_FAILED_TO_COMMIT
        txnMetaInfo.Alert =  &plan.Status{
            Code: perr.Code,
            Msg: perr.Msg,
        }
        job.Outlet.Send(&txnMetaInfo)

        St.log.WithError(err).Warn("CommitJob error")
    }

    // This releases the GRPC handler
    job.OnComplete <- err
}




func (St *Store) AddTxnSubscriber() chan pdi.UTID {
    sub := make(chan pdi.UTID, 1)
    
    St.subsMutex.Lock()
    St.subs = append(St.subs, sub)
    St.subsMutex.Unlock()

    return sub
}


func (St *Store) RemoveTxnSubscriber(inSub chan pdi.UTID) {

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


// DoQueryJob queues the given QueryJob
func (St *Store) DoQueryJob(job QueryJob) {

    err := St.CheckState()
    if err != nil {
        job.OnComplete <- err
        return
    }

    atomic.AddInt32(&St.numQueryJobs, 1)

    // TODO: use semaphore.NewWeighted() to bound the number of query jobs
    go func() {
        err := St.doQueryJob(job)

        if err != nil {
            St.log.WithError(err).Warn("doQueryJob() returned error")   
        }

        // This releases the GRPC handler
        job.OnComplete <- err

        atomic.AddInt32(&St.numQueryJobs, -1)
    }()
}



// DoSendJob queues the given SendJob
func (St *Store) DoSendJob(job SendJob) {

    err := St.CheckState()
    if err != nil {
        job.OnComplete <- err
        return
    }

    atomic.AddInt32(&St.numSendJobs, 1)

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



func (St *Store) doQueryJob(job QueryJob) error {

    var err error

    const (
        batchMax = 30
        batchBufSz = batchMax * pdi.UTIDBinarySz
    )
    var (
        batchBuf [batchBufSz]byte
        UTIDs [batchMax][]byte
    )
    for i := 0; i < batchMax; i++ {
        pos := i * batchBufSz
        UTIDs[i] = batchBuf[pos:pos+batchBufSz]
    }

    // Before we start the db txn (and effectively get locked to a db rev in time), subscribe this job to receive new commits
    if job.TxnQuery.ReportNewCommits {
        job.NewTxns = St.AddTxnSubscriber()
    }

    dbTxn := St.txnDB.NewTransaction(false)
    {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        opts.Reverse = job.TxnQuery.TimestampStart > job.TxnQuery.TimestampStop
        itr := dbTxn.NewIterator(opts)

        var (
            dir int
            keyBuf [pdi.UTIDBinarySz]byte
        )

        itr.Seek(  pdi.UTIDFromInfo(keyBuf[:0], job.TxnQuery.TimestampStart, nil))
        stopKey := pdi.UTIDFromInfo(keyBuf[:0], job.TxnQuery.TimestampStop,  nil)

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
                if totalCount == job.TxnQuery.MaxTxns {
                    dir = 0
                    break
                } else if batchCount == batchMax {
                    break
                }
            }
           
            if err == nil && batchCount > 0 {
                err = job.Outlet.Send(&pdi.TxnBatch{
                    UTIDs: UTIDs[:batchCount],
                })
            }
        }

        itr.Close()
    }

    dbTxn.Discard()
    dbTxn = nil

    if job.TxnQuery.ReportNewCommits {
        heartbeat := time.NewTicker(time.Second * 10)
        batchLag  := time.NewTicker(time.Millisecond * 300)

        batchCount := int32(0)
        wakeTimer := heartbeat.C

        for err == nil {

            select {

                case newTxn := <- job.NewTxns:
                    copy(UTIDs[batchCount], newTxn[:])
                    batchCount++

                    if batchCount == 1 {
                        wakeTimer = batchLag.C
                        for len(wakeTimer) > 0 {
                            <-wakeTimer
                        }
                    }
                        
                case <- wakeTimer:
                    err = job.Outlet.Send(&pdi.TxnBatch{
                        UTIDs: UTIDs[:batchCount],
                    })
                    wakeTimer = heartbeat.C
            }
        }

        St.RemoveTxnSubscriber(job.NewTxns)
    }

    return err
}