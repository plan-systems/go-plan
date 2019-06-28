package datastore

import (

    "fmt"
    //"sync/atomic"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    "bytes"
    //"encoding/json"
    "time"
    //"hash"
    //"crypto/rand"

    //"golang.org/x/sync/semaphore"

    //ds "github.com/ipfs/go-datastore"
	//dsq "github.com/ipfs/go-datastore/query"

    "github.com/dgraph-io/badger"

    "github.com/plan-systems/go-ptools"
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
    ptools.Context

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

    storeDesc := fmt.Sprintf("St:%v", St.Config.StorageEpoch.Name)
    St.SetLogLabel(storeDesc)

    return St
}

// Startup should be called once Datastore is preprared and ready to invoke the underlying implementation.
func (St *Store) Startup(
    inFirstTime bool,
) error {

    St.Info(0, "starting up store ", St.AbsPath)

    err := St.CtxStart(
        St.ctxStartup,
        nil,
        nil,
        St.ctxStopping,
    )

    return err
}


func (St *Store) ctxStartup() error {

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
    St.txnUpdates = make(chan txnUpdate, 8)
    St.CtxGo(func() {

        for update := range St.txnUpdates {
            St.subsMutex.Lock()
            for _, sub := range St.subs {
                sub <- update
            }
            St.subsMutex.Unlock()
        }
        St.Info(2, "commit notification exited")

        St.txnDB.Close()
        St.txnDB = nil

        St.acctDB.Close()
        St.acctDB = nil
    })
    //
    //
    //
    //
    St.DecodedCommits = make(chan CommitJob, 1)
    St.CtxGo(func() {

        // Process one commit job at a time
        for job := range St.DecodedCommits {            
            St.doCommitJob(job)
        }
        St.Info(2, "commit pipeline closed")

        // Cause all subs to fire, causing them to exit when they see St.OpState == Stopping
        St.txnUpdates <- txnUpdate{}
        close(St.txnUpdates)
    })

    return nil
}

func (St *Store) ctxStopping() {
    St.Info(2, "stopping")

    // This will initiate a close-cascade causing St.resources to be released
    if St.DecodedCommits != nil {
        close(St.DecodedCommits)
    }
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
                St.Infof(0, "creating account %v to receive deposit", plan.BinEncode(inAcctAddr))
            }
            err = dbTxn.Set(inAcctAddr, scrap[:acctSz]) 
            if err != nil {
                err = plan.Errorf(err, plan.StorageNotReady, "failed to update acct")
            }
        }
    }

    // If we get a deposit err, log it and proceed normally (i.e. the funds are lost forever)
    if err != nil {
        St.Errorf("error updating acct %v: %v", inAcctAddr, err)
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
                St.Infof(1, "committed txn %v", job.Txn.URIDStr())
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
                perr = plan.Error(err, plan.FailedToCommitTxns, "txn commit failed")
            }

            St.Errorf("CommitJob failed: %v, URID: %s", err, job.Txn.URIDStr())
        
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

    // TODO: use semaphore.NewWeighted() to bound the number of query jobs
    St.CtxGo(func() {

        err := St.doScanJob(job)
        
        if err != nil && St.CtxRunning() {
            St.Errorf("scan job error: %v", err)
        }

        job.OnComplete <- err
    })
}


// DoSendJob queues the given SendJob
func (St *Store) DoSendJob(job SendJob) {

    St.CtxGo(func() {
        err := St.doSendJob(job)
        job.OnComplete <- err
    })

}


// DoCommitJob queues the given CommitJob
func (St *Store) DoCommitJob(job CommitJob) error {

    err := St.CtxStatus()
    if err == nil {
        err = job.Txn.DecodeRawTxn(St.txnDecoder)
    }

    if err != nil {
        return err
    }

    St.DecodedCommits <- job

    return nil
}


// doSendJob sends the requested txns.
//
// Any error returned is related 
func (St *Store) doSendJob(job SendJob) error {

    dbTxn := St.txnDB.NewTransaction(false)
    {
        txn := &pdi.RawTxn{
            TxnMetaInfo: &pdi.TxnMetaInfo{
                TxnStatus: pdi.TxnStatus_FINALIZED,
            },
        }

        for _, URID := range job.URIDs {

            if ! St.CtxRunning() {
                break
            }

            txnOut := txn

            item, readErr := dbTxn.Get(URID)
            if readErr == nil {
                readErr = item.Value(func(inVal []byte) error {
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
            } else if readErr == badger.ErrKeyNotFound {
                txnOut = &pdi.RawTxn{
                    URID: URID,
                    TxnMetaInfo: &pdi.TxnMetaInfo{
                        TxnStatus: pdi.TxnStatus_MISSING,
                    },
                }
            }

            if readErr != nil {
                St.CtxOnFault(readErr, fmt.Sprintf("reading txn %v", pdi.URID(URID).Str()))
            } else {
                if sendErr := job.Outlet.Send(txnOut); sendErr != nil {
                    if ! St.CtxRunning() {
                        St.Warn("failed to send txn: ", sendErr)
                    }
                }
            }

            if St.CtxStatus() != nil {
                break
            }
        }
    }
    dbTxn.Discard()
    dbTxn = nil

    return nil
}


// doScanJob performs scan work.
//
// If an error is returned, it's bc there was a problem w/ the params (not b/c of an internal problem).  
// Internal problems will eventually cause this Context to stop if appropriate.
func (St *Store) doScanJob(job ScanJob) (jobErr error) {

    const (
        batchMax = 50
        batchBufSz = batchMax * pdi.URIDSz
    )
    var (
        URIDbuf [batchMax*pdi.URIDSz]byte
        URIDs   [batchMax][]byte
        batchDelay *time.Ticker
    )

    // Before we start the db txn (and effectively get locked to a db rev in time), subscribe this job to receive new commits
    if job.TxnScan.SendTxnUpdates {
        batchDelay = time.NewTicker(time.Millisecond * 300)
        
        job.txnUpdates = St.addTxnSubscriber()
    }

    statuses := make([]byte, batchMax)
    for i := 0; i < batchMax; i++ {
        pos := i * pdi.URIDSz
        URIDs[i] = URIDbuf[pos:pos + pdi.URIDSz]
    }

    heartbeat := time.NewTicker(time.Second * 28)
    
    {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        opts.Reverse = job.TxnScan.TimestampStart > job.TxnScan.TimestampStop

        var (
            scanDir int
            stopKey pdi.URIDBlob
            seekKey pdi.URIDBlob
            jobErr error
        )

        seekKey.URID().SetFromTimeAndHash(job.TxnScan.TimestampStart, nil)
        stopKey.URID().SetFromTimeAndHash(job.TxnScan.TimestampStop,  nil)

        if opts.Reverse {
            scanDir = -1
        } else {
            scanDir = 1
        }

        totalCount := int32(0)

        // Loop and send batches of txn IDs and interleave and txn updates.
        for jobErr == nil && St.CtxRunning() {

            // Track where the seek head started
            curScanPos := seekKey.ExtractTime()

            if scanDir != 0 {
                dbTxn := St.txnDB.NewTransaction(false)
                itr := dbTxn.NewIterator(opts)

                // Seek to the next key.  
                // If we're resuming, step to the next key after where we left off
                itr.Seek(seekKey[:])
                if  totalCount > 0 && itr.Valid() {
                    itr.Next()
                }

                batchCount := int32(0)
                
                for  ; itr.Valid() && St.CtxRunning(); itr.Next() {

                    item := itr.Item()
                    itemKey := item.Key()
                    if scanDir * bytes.Compare(itemKey, stopKey[:]) > 0 {
                        scanDir = 0
                        break
                    }

                    if len(itemKey) != pdi.URIDSz {
                        St.Errorf("encountered txn key len %d, expected %d", len(itemKey), pdi.URIDSz)
                        continue
                    }

                    copy(URIDs[batchCount], itemKey)
                    statuses[batchCount] = byte(pdi.TxnStatus_FINALIZED)

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

                if batchCount == 0 {
                    scanDir = 0
                } else {
                    copy(seekKey[:], URIDs[batchCount-1])

                    txnList := &pdi.TxnList{
                        URIDs:    URIDs[:batchCount],
                        Statuses: statuses[:batchCount],
                    }
                    jobErr = job.Outlet.Send(txnList)
                }
            }

            if jobErr != nil {
                break
            }

            // At this point, we've sent a healthy batch of scanned txn IDs and we need to also send any pending txn status updates.
            if job.TxnScan.SendTxnUpdates {
                batchCount := int32(0)
                fullWait := false

                // Do a full wait only if we're done with the txn scan (and only waiting for txn update msgs)
                wakeTimer := batchDelay.C
                if scanDir == 0 {
                    wakeTimer = heartbeat.C
                    fullWait = true
                }

                // Flush out any queued heartbeats
                for len(wakeTimer) > 0 {
                    <-wakeTimer
                }

                doneWaiting := false
                sendBatch := false
                
                for ! doneWaiting {

                    select {
                        case txnUpdate := <-job.txnUpdates:
                            if len(txnUpdate.URID) == pdi.URIDSz {
                                txnTime := txnUpdate.URID.ExtractTime()

                                // Only proceed if the txn update is within the time region implied w/ the current scan position.
                                // If the scan is complete, then the implied time region is complete, so we always proceed in that case.
                                proceed := true
                                switch scanDir {
                                    case +1:
                                        proceed = curScanPos >= txnTime
                                    case -1:
                                        proceed = txnTime >= curScanPos
                                }
                                if proceed {
                                    copy(URIDs[batchCount], txnUpdate.URID)
                                    statuses[batchCount] = byte(txnUpdate.TxnStatus)
                                    batchCount++

                                    // Once we get one, don't wait for the heartbeat to end -- only wait a short period for laggards and then send this batch.
                                    if fullWait {
                                        wakeTimer = batchDelay.C
                                        fullWait = false
                                        for len(wakeTimer) > 0 {
                                            <-wakeTimer
                                        }
                                    }
                                }
                            }

                        case <-wakeTimer:
                            doneWaiting = true
                            sendBatch = true

                        case <-St.Ctx.Done():
                            doneWaiting = true
                    }

                    if sendBatch || batchCount == batchMax {
                        txnList := &pdi.TxnList{
                            URIDs:    URIDs[:batchCount],
                            Statuses: statuses[:batchCount],
                        }
                        jobErr = job.Outlet.Send(txnList)
                        batchCount = 0
                    }
                }
            } else if scanDir == 0 {
                break
            }
        }
    }

    if job.TxnScan.SendTxnUpdates {
        St.removeTxnSubscriber(job.txnUpdates)
    }

    return jobErr
}


