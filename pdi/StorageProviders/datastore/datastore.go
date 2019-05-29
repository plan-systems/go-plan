package datastore

import (

    "fmt"
    "sync/atomic"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    "bytes"
    //"encoding/json"
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
    StorageEpoch            pdi.StorageEpoch        `json:"storage_epoch"`
}


// Store wraps a PLAN community UUID and a datastore
type Store struct {
    flow                        plan.Flow

    AgentDesc                   string    
    Config                      *StorageConfig

    AbsPath                     string

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

    commitScrap                 []byte

    numScanJobs                 int32
    numSendJobs                 int32
    numCommitJobs               int32
 
    txnDecoder                  pdi.TxnDecoder

    DefaultFileMode             os.FileMode

}


type txnUpdate struct {
    URID            pdi.URID
    TxnStatus       pdi.TxnStatus
}


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
        txnDecoder: NewTxnDecoder(true),
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

    storeDesc := fmt.Sprintf("Store %v", St.Config.StorageEpoch.Name)

    logE := log.WithFields(log.Fields{ 
        "impl": St.Config.ImplName,
        "path": St.AbsPath,
    })
    logE.Infof( "starting %v", storeDesc )

    err := St.flow.Startup(
        inCtx,
        storeDesc,
        St.internalStartup,
        St.internalShutdown,
    )

    return err
}



func (St *Store) internalStartup() error {

    /*
    // Load community info
    {
        pathname := path.Join(St.AbsPath, pdi.StorageEpochFilename)
        buf, err := ioutil.ReadFile(pathname)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "missing %s", pathname)
        }

        err = json.Unmarshal(buf, &St.StorageEpoch)
        if err != nil {
            return plan.Errorf(err, plan.ConfigFailure, "error unmarshalling %s", pathname)
        }
    }*/

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


    //
    //
    //
    //
    // Small buffer needed for the txn notifications to ensure that the txn writer doesn't get blocked
    St.flow.ShutdownComplete.Add(1)
    St.txnUpdates = make(chan txnUpdate, 8)
    go func() {
        for update := range St.txnUpdates {
            St.subsMutex.Lock()
            for _, sub := range St.subs {
                sub <- update
            }
            St.subsMutex.Unlock()
        }
        St.flow.Log.Info("1) commit notification exited")

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

        St.flow.ShutdownComplete.Done()
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
        St.flow.Log.Info("2) commit pipeline closed")

        // Cause all subs to fire, causing them to exit when they see St.OpState == Stopping
        St.txnUpdates <- txnUpdate{}
        close(St.txnUpdates)
    }()


    return nil
}


func (St *Store) internalShutdown() {

    // Wait until we're sure a commit didn't sneak in
    for {
        time.Sleep(100 * time.Millisecond)
        if atomic.LoadInt32(&St.numCommitJobs) == 0 {
            break
        }
    }

    St.flow.Log.Info("3) pending commits complete")

    // This will initiate a close-cascade causing St.resources to be released
    close(St.DecodedCommits)

}


// Shutdown -- see plan.Flow.Shutdown
func (St *Store) Shutdown(
    inReason string,
) {

    St.flow.Shutdown(inReason)

}



// CheckStatus -- see plan.Flow.CheckStatus
func (St *Store) CheckStatus() error {

    return St.flow.CheckStatus()

}


// IsRunning -- see plan.Flow.IsRunning
func (St *Store) IsRunning() bool {

    return St.flow.IsRunning()

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
    URIDs      [][]byte
    Outlet     pdi.StorageProvider_FetchTxnsServer
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
                St.flow.Log.Infof("creating account %v to receive deposit", pdi.Encode64(inAcctAddr))
            }
            err = dbTxn.Set(inAcctAddr, scrap[:acctSz]) 
            if err != nil {
                err = plan.Errorf(err, plan.StorageNotReady, "failed to update acct")
            }
        }
    }

    // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
    if err != nil {
        St.flow.Log.WithField( 
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

    curTime := time.Now().Unix()

    if err == nil {

        batch := NewTxnHelper(St.acctDB)
        for batch.NextAttempt() {

            // Debit the senders account (from Fuel and any transfers ordered)
            {
                kbSize := int64( len(job.Txn.RawTxn) >> 10 )

                err = St.updateAccount(
                    batch.Txn,
                    job.Txn.Info.From,
                    func (ioAcct *pdi.StorageAccount) error {

                        if ioAcct.OpBalance <= 0 {
                            return plan.Error(nil, plan.InsufficientPostage, "insufficient tx balance")
                        }

                        // Debit kb quota needed for txn
                        if ioAcct.KbBalance < kbSize {
                            return plan.Errorf(nil, plan.InsufficientPostage, "insufficient kb balance for txn size %dk", kbSize)
                        }

                        ioAcct.OpBalance--
                        ioAcct.KbBalance -= kbSize

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

            totalSz := len(job.Txn.RawTxn) + pdi.URIDTimestampSz
            if len(St.commitScrap) < totalSz {
                St.commitScrap = make([]byte, totalSz + 10000)
            }
            val := St.commitScrap[:totalSz]

            val[0] = byte(curTime >> 40)
            val[1] = byte(curTime >> 32)
            val[2] = byte(curTime >> 24)
            val[3] = byte(curTime >> 16)
            val[4] = byte(curTime >> 8)
            val[5] = byte(curTime)
            copy(val[pdi.URIDTimestampSz:], job.Txn.RawTxn)

            err := batch.Txn.Set(job.Txn.Info.URID, val)
            if err != nil {
                err = plan.Error(err, plan.StorageNotReady, "failed to write raw txn data to db")
            } else {
                St.flow.Log.Infof("committed txn %v", job.Txn.URIDstring())
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

            St.flow.Log.WithFields(log.Fields{
                "URID": job.Txn.URIDstring(),
            }).WithError(err).Warn("CommitJob error")
        
            err = perr
        }

        St.txnUpdates <- txnUpdate{
            job.Txn.Info.URID, 
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

    err := St.CheckStatus()
    if err != nil {
        job.OnComplete <- err
        atomic.AddInt32(&St.numScanJobs, -1)
        return
    }

    // TODO: use semaphore.NewWeighted() to bound the number of query jobs
    go func() {
        err := St.doScanJob(job)

        if err != nil && St.IsRunning() {
            St.flow.Log.WithError(err).Warn("scan job error")   
        }

        job.OnComplete <- err
        atomic.AddInt32(&St.numScanJobs, -1)
    }()
}


// DoSendJob queues the given SendJob
func (St *Store) DoSendJob(job SendJob) {
    atomic.AddInt32(&St.numSendJobs, 1)

    err := St.CheckStatus()
    if err != nil {
        job.OnComplete <- err
        atomic.AddInt32(&St.numSendJobs, -1)
        return
    }

    go func() {
        err := St.doSendJob(job)

        if err != nil && St.IsRunning() {
            St.flow.Log.WithError(err).Warn("send job error")   
        }

        job.OnComplete <- err
        atomic.AddInt32(&St.numSendJobs, -1)
    }()

}


// DoCommitJob queues the given CommitJob
func (St *Store) DoCommitJob(job CommitJob) error {
    atomic.AddInt32(&St.numCommitJobs, 1)

    err := St.flow.CheckStatus()
    if err == nil {
        err = job.Txn.DecodeRawTxn(St.txnDecoder)
    }

    if err != nil {
        atomic.AddInt32(&St.numSendJobs, -1)
        return err
    }

    St.txnUpdates <- txnUpdate{
        job.Txn.Info.URID, 
        pdi.TxnStatus_COMMITTING,
    }

    St.DecodedCommits <- job

    return nil
}



func (St *Store) doSendJob(job SendJob) error {

    var err error 

    dbTxn := St.txnDB.NewTransaction(false)
    {
        txn := pdi.RawTxn{
            TxnMetaInfo: &pdi.TxnMetaInfo{
                TxnStatus: pdi.TxnStatus_FINALIZED,
            },
        }

        for _, URID := range job.URIDs {
            txnOut := &txn

            item, dbErr := dbTxn.Get(URID)
            if dbErr == nil {
                txnOut = &txn

                err = item.Value(func(inVal []byte) error {
                    t := int64(inVal[0]) << 40
                    t |= int64(inVal[1]) << 32
                    t |= int64(inVal[2]) << 24
                    t |= int64(inVal[3]) << 16
                    t |= int64(inVal[4]) <<  8
                    t |= int64(inVal[5])
                    txnOut.TxnMetaInfo.ConsensusTime = t
                    txnOut.Bytes = inVal[pdi.URIDTimestampSz:]
                    return nil
                })
            } else if dbErr == badger.ErrKeyNotFound {
                // TODO: send alert that URID not found?
            } else {
                err = dbErr
            }

            if err != nil {
                St.flow.Log.WithError(err).Warn("failed to read txn")
                continue
            }

            err = St.CheckStatus()
            if err == nil && txnOut != nil {
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
        batchMax = 50
        batchBufSz = batchMax * pdi.URIDSz
    )
    var (
        URIDbuf [batchMax * pdi.URIDSz]byte
        URIDs [batchMax][]byte
    )
    statuses := make([]byte, batchMax)
    for i := 0; i < batchMax; i++ {
        statuses[i] = byte(pdi.TxnStatus_FINALIZED)

        pos := i * pdi.URIDSz
        URIDs[i] = URIDbuf[pos:pos + pdi.URIDSz]
    }

    heartbeat := time.NewTicker(time.Second * 28)
    var batchLag *time.Ticker

    // Before we start the db txn (and effectively get locked to a db rev in time), subscribe this job to receive new commits
    if job.TxnScan.SendTxnUpdates {
        batchLag = time.NewTicker(time.Millisecond * 300)

        job.txnUpdates = St.addTxnSubscriber()
    }

    {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        opts.Reverse = job.TxnScan.TimestampStart > job.TxnScan.TimestampStop

        var (
            scanDir int
            stopKeyBuf [pdi.URIDSz]byte
            seekKeyBuf [pdi.URIDSz]byte
        )

        seekKey := pdi.URIDFromInfo(seekKeyBuf[:], job.TxnScan.TimestampStart, nil)
        stopKey := pdi.URIDFromInfo(stopKeyBuf[:], job.TxnScan.TimestampStop,  nil)

        if opts.Reverse {
            scanDir = -1
        } else {
            scanDir = 1
        }

        totalCount := int32(0)

        // Loop and send batches of txn IDs and interleave and txn updates.
        for  err == nil && (scanDir != 0 || job.TxnScan.SendTxnUpdates) {

            if scanDir != 0 {
                dbTxn := St.txnDB.NewTransaction(false)
                itr := dbTxn.NewIterator(opts)

                // Seek to the next key.  
                // If we're resuming, step to the next key after where we left off
                itr.Seek(seekKey)
                if  totalCount > 0 && itr.Valid() {
                    itr.Next()
                }

                batchCount := int32(0)
                
                for  ; itr.Valid(); itr.Next() {

                    if err = St.CheckStatus(); err != nil {
                        break
                    }

                    item := itr.Item()
                    itemKey := item.Key()
                    if scanDir * bytes.Compare(itemKey, stopKey) > 0 {
                        scanDir = 0
                        break
                    }

                    if len(itemKey) != pdi.URIDSz {
                        St.flow.Log.Warnf("encountered txn key len %d, expected %d", len(itemKey), pdi.URIDSz)
                        continue
                    }

                    copy(URIDs[batchCount], itemKey)

                    batchCount++
                    totalCount++
                    if totalCount == job.TxnScan.MaxTxns {
                        scanDir = 0
                        break
                    } else if batchCount == batchMax {
                        break
                    }
                }

                itr.Close()
                dbTxn.Discard()
                dbTxn = nil

                if err == nil {
                    err = St.CheckStatus()
                }

                if batchCount == 0 {
                    scanDir = 0
                } else if err == nil {
                    err = job.Outlet.Send(&pdi.TxnList{
                        URIDs:    URIDs[:batchCount],
                        Statuses: statuses[:batchCount],
                    })

                    copy(seekKey, URIDs[batchCount-1])
                }

            }

            // At this point, we've sent a healthy batch of scanned txn IDs and we need to also send any pending txn status updates.
            {
                batchCount := int32(0)
                sent := false

                // Wait the full heartbeat time unless we get a txn update in which case send out the batch after a short delay.
                wakeTimer := heartbeat.C

                // Flush out any queued heartbeats
                for len(wakeTimer) > 0 {
                    <-wakeTimer
                }

                for err == nil && ! sent {

                    select {
                        case txnUpdate := <- job.txnUpdates:
                            if len(txnUpdate.URID) == pdi.URIDSz {
                                copy(URIDs[batchCount], txnUpdate.URID)
                                statuses[batchCount] = byte(txnUpdate.TxnStatus)
                                batchCount++

                                // Once we one, don't wait for the heartbeat to end -- only wait a short period for laggards and then send this batch.
                                if batchCount == 1 {
                                    wakeTimer = batchLag.C
                                    for len(wakeTimer) > 0 {
                                        <-wakeTimer
                                    }
                                }
                            }
                                
                        case <- wakeTimer:
                            err = job.Outlet.Send(&pdi.TxnList{
                                URIDs: URIDs[:batchCount],
                                Statuses: statuses[:batchCount],
                            })
                            sent = true

                        case <- St.flow.Ctx.Done():
                            err = St.CheckStatus()
                    }
                }

            }

        }
    }

    if job.TxnScan.SendTxnUpdates {
        St.removeTxnSubscriber(job.txnUpdates)
    }

    return err
}


