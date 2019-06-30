package repo

import (
    "os"
    "path"
    "fmt"
    "io"
    "io/ioutil"
    //"strings"
    "sync"
    "context"
    "time"
    "bytes"
    //"sort"
    //"encoding/hex"
    "encoding/json"
    //"e
    

 	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"

    ds "github.com/plan-systems/plan-pdi-local/datastore"

    "github.com/plan-systems/plan-core/tools"
    "github.com/plan-systems/plan-core/pdi"
    "github.com/plan-systems/plan-core/plan"
    "github.com/plan-systems/plan-core/ski"

    "github.com/dgraph-io/badger"
)


// Config are params needed in order to run/start a CommunityRepo
// TODO: where do these settings go?  StorageEpoch 
type Config struct {

    // Number of storage provider faults/errors before this repo disconnects and requires manual override
    MaxStorageProviderFaults int32              `json:"max_storage_provider_faults"`   

    // Number of seconds the sync scanner looks backward to look for txns it's missed
    ReverseScanStep         int                 `json:"reverse_scan_step"` 
}


// CommunityRepoState stores repo state
type CommunityRepoState struct {
    LastTxnTimeRead         int64               `json:"last_txn_time_read"`     // Time index where repo should resume reading a community StorageProvider

    //LatestCommunityEpoch    pdi.CommunityEpoch  `json:"lastest_community_epoch"`

    Services                []*plan.ServiceInfo  `json:"services"`
}


// CommunityRepo wraps a community's data repository and responds to queries for the given community.
type CommunityRepo struct {
    tools.Context

    GenesisSeed             GenesisSeed    
    Config                 *Config
    HomePath                string

    State                   CommunityRepoState

    // This will move and is just a hack for now
    //activeSession           ClientSession

    DefaultFileMode         os.FileMode

    txnDB                   *badger.DB      // Complete record of community txns (by URID); i.e. a replica "follower" of StorageProvider

    chMgr                   *ChMgr

    //txnsDeferred            ds.Datastore        // Values point to txn?


    spSyncStatus            spSyncStatus
    spSyncActive            bool
    spSyncWakeup            chan struct{}
    spSyncWorkers           sync.WaitGroup

    spClient                pdi.StorageProviderClient
    spInfo                 *pdi.StorageInfo
    spCancel                context.CancelFunc
    spContext               context.Context

    //spCommitContext         context.Context
    //spCommitCancel          context.CancelFunc

  //  spScanContext           context.Context
  //  spScanCancel            context.CancelFunc

    //spCommitTxns            pdi.StorageProvider_CommitTxnsClient

    unpacker                ski.PayloadUnpacker


    txnsToDecode            chan pdi.RawTxn
    txnsToWrite             chan *pdi.PayloadTxnSet
    txnsToCommit            chan *pdi.PayloadTxnSet

    txnsToFetch             chan *pdi.TxnList         // URIDs to be fetched from the SP (that the repo needs)
    txnCollater             pdi.TxnCollater

    txnWorklist             *badger.DB      // Status info by txn URID

    entriesToMerge          chan *chEntry

    // Used to save on allocations
    commKeyRef              ski.KeyRef

    commSessionsMutex       sync.RWMutex
    commSessions            []*CommunityCrypto

    memberSessMgr           tools.Context

    //communitySKI            ski.Session
    //communitySKICond        *sync.Cond
    //communitySKIMutex       sync.RWMutex

    //txnScanning             sync.WaitGroup

    //pipelines               sync.WaitGroup


    // Used for unpacking txns intenrally
    tmpCrypt            pdi.EntryCrypt



/*


    // Newly authored entries from active sessions on this pnode that are using this CommunityRepo.
    // These entries are first validated/processed as if they came off the wire, merged with the local db, and committed to the active storage sessions.
    authoredInbox           chan *pdi.EntryCrypt

    // deamonSKIs makes it possible for community public encrypted data to be decrypted, even when there are NO
    //     client sessions open.  Or, a community repo may have its security settings such that the community keyring
    //     is dropped when there are no more active client sessions open.
    //deamonSKIs              []plan.SyncSKI

    // Includes both open and loaded channels
    loadedChannels          ChannelStoreGroup

    // This group is checked first when looking up a channel and is a group of channels that are open/hot in one way or another.
    openChannels            ChannelStoreGroup*/

}



// NewCommunityRepo creates a CommunityRepo for use.
//
// If inSeed is set, this repo is being instantiated for the first time
func NewCommunityRepo(
    inHomePath string,
    inSeed *RepoSeed,
) (*CommunityRepo, error) {
    
    var err error

    CR := &CommunityRepo{
        DefaultFileMode: plan.DefaultFileMode,
        unpacker: ski.NewUnpacker(true),
        HomePath: inHomePath,
        spSyncWakeup: make(chan struct{}, 5),
        spSyncActive: false,
        spSyncStatus: spSyncStopped,
    }

    name := fmt.Sprintf(path.Base(CR.HomePath))
    CR.SetLogLabel(name)

    seedPathname := CR.GenesisSeedPathname()

    // If we're seeding this repo, write out critical files.
    if inSeed != nil {

        // Write out the initial state
        CR.State = CommunityRepoState{
            Services: inSeed.Services,
        }
        err = CR.flushState()

        // Write out the signed genesis seed
        if err == nil {
            err = ioutil.WriteFile(seedPathname, inSeed.SignedGenesisSeed, CR.DefaultFileMode)
        }
    } 

    if err == nil {
        if seed, err := LoadAndVerifyGenesisSeed(seedPathname); err == nil {
            CR.GenesisSeed = *seed
        }
    }

    if err == nil {
        CR.chMgr = NewChMgr(inHomePath, CR)
        CR.commKeyRef.KeyringName = CR.GenesisSeed.StorageEpoch.CommunityKeyringName()
    }

    return CR, err
}





func (CR *CommunityRepo) flushState() error {

    buf, err := json.MarshalIndent(&CR.State, "", "\t")
    if err == nil {
        pathname := path.Join(CR.HomePath, "RepoState.json")

        err = ioutil.WriteFile(pathname, buf, CR.DefaultFileMode)
    }

    return err
}

// Startup starts this repo
func (CR *CommunityRepo) Startup() error {

    if CR.CtxRunning() {
        panic("repo is already running")
    }

    err := CR.CtxStart(
        CR.ctxStartup,
        nil,
        nil,
        CR.ctxStopping,
    )

    return err
}

// GenesisSeedPathname returns the pathname of the signed genesis seed.
func (CR *CommunityRepo) GenesisSeedPathname() string {
    return path.Join(CR.HomePath, "GenesisSeed.signed")
} 

func (CR *CommunityRepo) ctxStartup() error {
    
    pathname := path.Join(CR.HomePath, "RepoState.json")
    buf, err := ioutil.ReadFile(pathname)
    if err == nil { 
        err = json.Unmarshal(buf, &CR.State)
    }
    if err != nil {
        return err
    }

    opts := badger.DefaultOptions
    opts.Dir = path.Join(CR.HomePath, "txnDB")
    opts.ValueDir = opts.Dir

    if CR.txnDB, err = badger.Open(opts); err != nil {
        return plan.Error(err, plan.StorageNotReady, "CommunityRepo.txnDB.Open() failed")
    }


    //
    //
    //
    //
    // txn committer
    //
    // Receives txns ready to be committed to the community's storage
    CR.txnsToCommit = make(chan *pdi.PayloadTxnSet, 16)
    CR.CtxGo(func() {

        for payload := range CR.txnsToCommit {

            for _, txn := range payload.Segs {
                _, err := CR.spClient.CommitTxn(CR.spContext, &pdi.RawTxn{Bytes: txn.RawTxn})
                if err != nil {
                    CR.Warn("StorageProvider.CommitTxn() failed: ", err)
                }
            }

            CR.Infof(1, "committed payload %v", payload.PayloadTID.TID().SuffixStr())

            pdi.RecycleTxnSet(payload)

            // TODO update commit log
            // mark txn as committed
            // mark txn as conformed/witnessed when seen later on a scan
            // All possible txn meta info: 
            //    - txn authored locally?
            //    - (if NOT authored locally), when was txn received?
            //    - (if authored locally), txn successfully committed?
            //    - (if authored locally and sucessfully committed), txn ID witnessed at a later time?
            //     - idea: maintain separate table of txns:
            //          - locally authored txns to be committed, confirmed committed, and confirmed witnessed? (3 lists)
            //          - txns that have yet to be processed into channels (e.g. post to channel that doesn't exist yet)

        }

        // TODO: fix this so fetching stop asap but commit is last to disconnect.
        // Or, we can stop asap once we have txn logging such that uncommited txns get to the server
        CR.spSyncStop()

        CR.flushState()

        CR.txnDB.Close()
        CR.txnDB = nil

        CR.Info(0, "shutdown complete")
    
    })
    //
    //
    //
    //
    // txnsToWrite processor
    //
    // Writes incoming raw txns to the txn DB
    CR.txnsToWrite = make(chan *pdi.PayloadTxnSet, 8)
    CR.CtxGo(func() {

        for payload := range CR.txnsToWrite {
            wb := CR.txnDB.NewWriteBatch()
            N := len(payload.Segs)
            for _, seg := range payload.Segs {
                err = wb.Set(seg.Info.URID, seg.RawTxn)
                if err != nil {
                    // TODO log err
                }
            }
            err = wb.Flush()
            if err != nil {
                // TODO log err
            }
            CR.Infof(1, "stored payload %v (%d txns)", payload.PayloadTID.TID().SuffixStr(), N)

            // Commits underlying txns if they've been locally authored
            if payload.NewlyAuthored {
                CR.txnsToCommit <- payload
            }
        }

        close(CR.txnsToCommit)
    })
    //
    //
    //
    //
    // inbound pdi entry processor
    //
    // Processes a new pdi.EntryCrypt and dispatches it to the appropriate channel pipeline
    CR.entriesToMerge = make(chan *chEntry, 1)
    CR.CtxGo(func() {

        for entry := range CR.entriesToMerge {
            err := CR.DecryptAndMergeEntry(entry)
            if err != nil {
                CR.Warn("failed to process entry: ", err)
            }
        }

        // Now that all entries have been inserted into the chMgr, we can shut that down
        CR.chMgr.CtxStop(CR.CtxStopReason(), nil)
        CR.chMgr.CtxWait()

        // End all community keyring sessions now that we're done decrypting.
        {
            CR.commSessionsMutex.Lock()
            N := len(CR.commSessions)
            for i := 0; i < N; i++ {
                CR.commSessions[i].EndSession(CR.CtxStopReason())
                CR.commSessions[i] = nil
            }
            CR.commSessions = nil
            CR.commSessionsMutex.Unlock()
        }

        close(CR.txnsToWrite)
    })
    //
    //
    //
    //
    // txnsToDecode processor
    //
    // Decodes incoming raw txns and inserts them into the collator, which performs callbacks to merge payloads (entries)
    CR.txnsToDecode = make(chan pdi.RawTxn, 1)
    CR.CtxGo(func() {

        // TODO: choose different encoder based on spInfo
        txnDecoder := ds.NewTxnDecoder(false)

        for txnIn := range CR.txnsToDecode {

            txnSet, err := CR.txnCollater.DecodeAndCollateTxn(txnDecoder, &txnIn)
            if err != nil {
                CR.Warnf("error processing txn %v: %v", txnIn.URID, err)
            } else {
                payloadEncoding := txnSet.PayloadEncoding()

                switch payloadEncoding {
                    case plan.Encoding_Pb_EntryCrypt:
                        entry := NewChEntry(entryFromStorageProvider)
                        entry.PayloadTxnSet = txnSet
                        CR.entriesToMerge <- entry
                    default:
                        CR.Warn("encountered unhandled payload encoding: ", payloadEncoding)
                }
            }
        }

        close(CR.entriesToMerge)
    })
    //
    //
    //
    //
    // txn requester
    //
    // Dispatches txn ID requests to the community's (remote) StorageProvider(s), managing connections etc.
    CR.txnsToFetch = make(chan *pdi.TxnList, 16)
    CR.CtxGo(func() {

        for txnList := range CR.txnsToFetch {
            
            // Drop requests until the channel closes.  This prevents shutdown being blocked b/c of a full channel.
            if ! CR.CtxRunning() {
                continue
            }

            spReader, err := CR.spClient.FetchTxns(CR.spContext, txnList)
            for err == nil && CR.CtxRunning() {
                var txnIn *pdi.RawTxn
                txnIn, err = spReader.Recv()
                if txnIn != nil {
                    CR.txnsToDecode <- *txnIn
                }
            }
            if err != nil && err != io.EOF {
                CR.Warn("StorageProvider.FetchTxns() error: ", err)
            }
        }

        close(CR.txnsToDecode)
    })

    if err = CR.chMgr.Startup(); err != nil {
        return err
    }
    
    CR.memberSessMgr.SetLogLabel(CR.GetLogLabel() + "-msMgr")
    CR.memberSessMgr.CtxStart(
        nil,
        nil,
        nil,
        nil,
    )
    
    CR.CtxGo(CR.spSyncController)

    return nil
}


func (CR *CommunityRepo) ctxStopping() {

    // First, end all member sessions (and channel sessions inside each member session)
    CR.memberSessMgr.CtxStop(CR.CtxStopReason(), nil)
    CR.memberSessMgr.CtxWait()

    // This will cause the fetch, then decode, then merge routines to stop
    if CR.txnsToFetch != nil {
        close(CR.txnsToFetch)
    }
}


func (CR *CommunityRepo) spSyncActivate() {

    CR.Info(1, "StorageProvider sync ACTIVATING")
    
    // Scanning will stop once it sees the CR is shutting down so wait until we know for sure that scanning is done so we know no new txns are requested
    CR.spSyncActive = true
    CR.spSyncWake()
}



func (CR *CommunityRepo) spSyncStop() {

    CR.Info(1, "StorageProvider sync DEACTIVATING")

    // Scanning will stop once it sees the CR is shutting down so wait until we know for sure that scanning is done so we know no new txns are requested
    CR.spSyncActive = false
    CR.spSyncWake()

    CR.spSyncWorkers.Wait()

    CR.Info(1, "StorageProvider sync DEACTIVATED")

}


func (CR *CommunityRepo) spSyncWake() {

    if len(CR.spSyncWakeup) <= 1 {
        CR.spSyncWakeup <- struct{}{}
    }
}


// disconnectFromStorage disconnects from the currently connected StorageProvider
func (CR *CommunityRepo) disconnectFromStorage() {

    CR.spSyncStatus = spSyncStopping
    if CR.spCancel != nil {
        CR.spCancel()
        CR.spCancel = nil
    }

    CR.spSyncWorkers.Wait()
    CR.spClient = nil
    CR.spContext = nil
    CR.spInfo = nil
    CR.spSyncStatus = spSyncStopped
    CR.spSyncWake()
}


func (CR *CommunityRepo) spSyncController() {

    sleep := true

    // The objective is for the StorageProvider sync subsystem to be active or inactive (stopped).
    // That is a complex network evolution, so this loop serves to control the smaller pieces,
    //    and its goal state is governed by CR.spSyncActive, a bool set if the system should be running
    //    or should be transitioned to spSyncStopped.
    for {

        // Are we stopping?  Before we break this loop, we must get back to a stopped state.
        if ! CR.CtxRunning() {
            if ! CR.spSyncActive {
                CR.spSyncActive = false
            }
            
            // Wait until we're in the ground state before exiting
            if CR.spSyncStatus == spSyncStopped {
                break
            }
        } else if sleep {

            // Wait until someone wakes us up!
            select {
                case <-CR.spSyncWakeup:
            }
        } else {
            //time.Sleep(1 * time.Second)
        }

        sleep = false

        // Are in active sync mode?  
        // If yes, initiate connection (or go back to sleep and expect to be woken when we're in the right state to connect)
        // If not, disconnect from SPs and wait until we've returned to a proper inactive/shutdown state. 
        if CR.spSyncActive {
            switch CR.spSyncStatus {
                case spSyncStopped:
                    CR.connectToStorage()
                default:
                    sleep = true
            }
        } else {
            switch CR.spSyncStatus {
                case spSyncStopped:
                    sleep = true
                case spSyncStopping:
                    sleep = true
                default:
                    CR.disconnectFromStorage()
            }
        }
    }

    CR.Info(1, "StorageProvider sync mgr exiting")
}


func (CR *CommunityRepo) connectToStorage() {

    CR.Info(1, "connecting to StorageProvider")

    CR.spSyncStatus = spSyncConnecting

	CR.spContext, CR.spCancel = context.WithCancel(CR.Ctx)

    addr := CR.State.Services[0].Addr
	clientConn, err := grpc.DialContext(CR.spContext, addr, grpc.WithInsecure())
	if err != nil {
        err = plan.Errorf(err, plan.FailedToConnectStorageProvider, "grpc.Dial() failed with addr %v", addr)
	}

    if err == nil {
	    CR.spClient = pdi.NewStorageProviderClient(clientConn)
    }
		
    if err == nil {
        var header, trailer metadata.MD
        CR.spInfo, err = CR.spClient.StartSession(
            CR.spContext, 
            &pdi.SessionReq{
                StorageEpoch: CR.GenesisSeed.StorageEpoch,
            },
            grpc.Header(&header), 
            grpc.Trailer(&trailer),
        )

        if err == nil {
            CR.spContext, err = tools.TransferSessionToken(CR.spContext, trailer)
            if err != nil {
                err = plan.Error(err, plan.FailedToConnectStorageProvider, "TransferSessionToken() failed")
            }
        }
    }

    if err != nil {
        CR.Error("failed to connect to StorageProvider: ", err)
        CR.disconnectFromStorage()
        
        retry := time.NewTimer(10 * time.Second)
        select {
            case <-retry.C:
            case <-CR.CtxStopping():
        }
        CR.spSyncWake()

    } else {
        CR.spSyncWorkers.Add(1)
        CR.spSyncStatus = spSyncForwardScan
        go CR.forwardTxnScanner()
    }
}


/*
// getMostRecentTxnTime blocks and retrieves the time index of the most recent txn
func (CR* CommunityRepo) getMostRecentTxnTime() {

    int64 latestTime

    err := db.View(func(dbTx *badger.Txn) error {

        it := dbTx.NewIterator(badger.IteratorOptions{
            PrefetchValues: false,
            Reverse: true,
            PrefetchSize: 1,
        })
        
        it.Rewind(); it.Valid(); it.Next() {

        }
        defer it.Close()
        for it.Rewind(); it.Valid(); it.Next() {
        item := it.Item()
        k := item.Key()
        err := item.Value(func(v []byte) error {
            fmt.Printf("key=%s, value=%s\n", k, v)
            return nil
        })
        if err != nil {
            return err
        }
        }
        return nil
        })

	opt := badger.DefaultIteratorOptions
    opt.PrefetchValues = false

	prefix := []byte(q.Prefix)
	opt.PrefetchValues = !q.KeysOnly

	// Special case order by key.
	orders := q.Orders
	if len(orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			// Already ordered by key.
			orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			orders = nil
			opt.Reverse = true
		}
	}

	txn := t.txn

	it := txn.NewIterator(opt)
	if len(q.SeekPrefix) > 0 {
		it.Seek([]byte(q.SeekPrefix))
	} else {
		it.Seek(prefix)
	}
	if q.Offset > 0 {
		for j := 0; j < q.Offset; j++ {
			it.Next()
		}
	}

}
*/



/*
type syncMode int
const (
	syncReverseScan syncMode = iota
    syncForwardScan
)
*/

type spSyncStatus int32
const (
	spSyncStopped spSyncStatus = iota
    spSyncConnecting
    spSyncForwardScan
    spSyncReverseScan
    spSyncStopping
)


func (CR *CommunityRepo) filterNeededTxns(ioTxnList *pdi.TxnList) error {

    var err error

    count := 0
    N := len(ioTxnList.URIDs)
    Ns := len(ioTxnList.Statuses)
    if N != Ns {
        if N > Ns {
            N = Ns
        }
        err = plan.Error(nil, plan.StorageNotConsistent, "received inconsistent TxnList")
        CR.Warn(err)
        return err
    }

    if N > 0 {
        dbTxn := CR.txnDB.NewTransaction(false)
        for i := 0; i < N && err == nil; i++ {
            txnStatus := pdi.TxnStatus(ioTxnList.Statuses[i])
            URID := ioTxnList.URIDs[i]
            
            switch txnStatus {
                case pdi.TxnStatus_COMMITTED:
                    fallthrough
                case pdi.TxnStatus_FINALIZED:
                    _, itemErr := dbTxn.Get(URID)
                    if itemErr == nil {
                        // entry exists; no op!
                    } else if itemErr == badger.ErrKeyNotFound {
                        ioTxnList.URIDs[count] = URID
                        count++
                    } else {
                        itemErr = plan.Errorf(itemErr, plan.TxnDBNotReady, "error reading txn DB key %v", pdi.URID(URID).Str())
                        CR.CtxOnFault(itemErr, "reading txn db key")
                    }
            }
        }
        dbTxn.Discard()
    }

    ioTxnList.URIDs = ioTxnList.URIDs[:count]
    ioTxnList.Statuses = ioTxnList.Statuses[:0]

    return nil
}

            

type spScanMode int
const (
    initialBackScan spScanMode = iota
    
)


//
// It also starts a URID scan from that time and earlier (in reverse order). As it encounters unwitnessed txns,
// it will explicitly fetch them.  When the URID txn correspondence is sufficient and convincing, the reverse scan
// is stopped, the fetch queue is emptied, and only the forward reader will eventually remain.
func (CR *CommunityRepo) backwardTxnScanner() {


}

//
// Continuously queries the community's storage provider(s) for txns (or txn status) this repo does not yet have.
// When it receives URID updates, reconciles that with the repo's txn db, and sends off requests for missing txns.
func (CR *CommunityRepo) forwardTxnScanner() {

    doScan := true
    for doScan {
        CR.Info(1, "starting forward txn scan")

        scanner, err := CR.spClient.Scan(
            CR.spContext,
            &pdi.TxnScan{
                TimestampStart: CR.State.LastTxnTimeRead,
                TimestampStop: pdi.URIDTimestampMax,
                SendTxnUpdates: true,
            },
        )
        if err != nil {
            CR.Warn("StorageProvider.Scan() error: ", err)
        }

        // Receive batches of URIDs and see which ones we don't have
        for err == nil {
            var txnList *pdi.TxnList
            txnList, err = scanner.Recv()

            if err != nil && CR.spContext.Err() == nil {
                CR.Warn("StorageProvider.Scan() recv error: ", err)
            } else if txnList != nil {

                // Filter for txn we need
                CR.filterNeededTxns(txnList)

                // Request txns we're missing
                if len(txnList.URIDs) > 0 {
                    CR.txnsToFetch <- txnList 
                }
            }
        }

        select {
            case <-CR.spContext.Done():
                doScan = false
            case <-time.After(5 * time.Second):
        }
    }

    CR.Info(2, "forwardTxnScanner done")

    CR.spSyncWorkers.Done()
}

// DecryptAndMergeEntry decrypts the given entry and then merges it with this repo.
//
// If an error is returned, it means the entry is malformed (and never can be merged).
func (CR *CommunityRepo) DecryptAndMergeEntry(entry *chEntry) error {

    tmpCrypt := &CR.tmpCrypt
    err := entry.PayloadTxnSet.UnmarshalPayload(tmpCrypt)
    if err != nil {
        return plan.Error(err, plan.UnmarshalFailed, "payload txn set unmarshal failed")
    }

    commEpoch := CR.FetchCommunityEpoch(tmpCrypt.CommunityEpochID, true)

    // TODO: have txn holding tank for txns that can't decode b/c the epoch isn't live or not found yet
    if commEpoch == nil {
        return plan.Errorf(nil, plan.CommunityEpochNotFound, "community epoch %v not found", plan.BinEncode(tmpCrypt.CommunityEpochID))
    }

    var payload ski.SignedPayload

    {
        var decryptOut *ski.CryptOpOut

        // to decrypt an incoming entry, we need have the community keyring on hand.  
        // This is only available if a memeber is logged in *or* a member has offered their community keyring 
        //    to be retained for the explicit purpose of processing entries with no one logged in.
        CR.commSessionsMutex.RLock()
        {
            CR.commKeyRef.PubKey = commEpoch.KeyInfo.PubKey
            decryptArgs := ski.CryptOpArgs{
                CryptOp: ski.CryptOp_DECRYPT_SYM,
                OpKey: &CR.commKeyRef,
                BufIn: tmpCrypt.PackedEntry,
            }

            for _, commSess := range CR.commSessions {
                var decryptErr error
                decryptOut, decryptErr = commSess.Keys.DoCryptOp(&decryptArgs)
                if decryptErr != nil {
                    break
                }
                if err != nil && plan.IsError(decryptErr, plan.KeyringNotFound, plan.KeyEntryNotFound) {
                    CR.Warnf("error decrypting incoming entry: %v", decryptErr)
                }
            }
        }
        CR.commSessionsMutex.RUnlock()

        if decryptOut == nil {
            err = plan.Error(nil, plan.CommunityKeyNotAvailable, "community key not available")
        } else {
            err = CR.unpacker.UnpackAndVerify(
                decryptOut.BufOut,
                &payload,
            )
        }
    }

    if err == nil {
        err = entry.AssignFromDecrytedEntry(&payload)
    }
    
    if err == nil {
        if commEpoch.SigningCryptoKit != payload.Signer.CryptoKit {
            err = plan.Errorf(nil, plan.ViolatesCommunityEpoch, "entry signed with unacceptable CryptoKitID: %d", payload.Signer.CryptoKit)
            entry.ThrowMalformed(err)
        }
    }
        
    if err != nil {
        return err
    }



    /* TODO: perform timestamp sanity checks
    if eip.EntryInfo.TimeAuthored < eip.CR.Info.TimeCreated.UnixSecs {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp earlier than community creation timestamp")
    }
    if eip.timeStart.UnixSecs - eip.entryHeader.TimeAuthored + eip.CR.Info.MaxPeerClockDelta < 0 {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp too far in the future")
    }*/


    // If this entry appears to be a genesis entry, we def want to verify that.  :)
    if entry.IsWellFormed() {
        isGenesisEntry := entry.Info.AuthorEntryID().IsNil()
        if isGenesisEntry {
            entryID := entry.Info.EntryID()

            found := false
            for _, genesisEntryID := range CR.GenesisSeed.StorageEpoch.GenesisEntryIDs {
                if bytes.Equal(entryID, genesisEntryID) {
                    entry.AddFlags(EntryFlag_GENESIS_ENTRY_VERIFIED)
                    found = true
                    break
                }
            }
            if ! found {
                entry.ThrowMalformed(
                    plan.Errorf(nil, plan.GenesisEntryNotVerified, "genesis entry %v not found", entryID),
                )
            }
        }
    }

    CR.chMgr.QueueEntryForMerge(
        commEpoch,
        entry,
    )
 
    // At this point, the PDI entry's signature has been verified
    return err
}


// OpenMemberSession starts a new session for the given member
func (CR *CommunityRepo) OpenMemberSession(
    inSessReq *MemberSessionReq, 
    inMsgOutlet Repo_OpenMemberSessionServer,
) (*MemberSession, error) {

    // Sanity check
    if ! bytes.Equal(inSessReq.CommunityID, CR.GenesisSeed.StorageEpoch.CommunityID) {
        return nil, plan.Error(nil, plan.AssertFailed, "community ID does not match repo's ID")
    }

    ms, err := NewMemberSession(
        CR,
        inSessReq,
        inMsgOutlet,
    )
    if err != nil {
        return nil, err
    }

    CR.memberSessMgr.CtxAddChild(ms, nil)

    return ms, nil
}


// LatestCommunityEpoch -- see interface MemberHost
func (CR *CommunityRepo) LatestCommunityEpoch() *pdi.CommunityEpoch {
    chCE := CR.chMgr.FetchCommunityEpochsChannel()
    if chCE != nil {
        return chCE.LatestCommunityEpoch()
    } 

    return CR.GenesisSeed.CommunityEpoch
}

// LatestStorageEpoch -- see interface MemberHost
func (CR *CommunityRepo) LatestStorageEpoch() pdi.StorageEpoch {
    return *CR.GenesisSeed.StorageEpoch
}

// FetchCommunityEpoch returns the CommunityEpoch with the matching epoch ID
func (CR *CommunityRepo) FetchCommunityEpoch(inEpochID []byte, inLiveOnly bool) *pdi.CommunityEpoch {

    var commEpoch *pdi.CommunityEpoch

    if len(inEpochID) == plan.TIDSz {
        chCE := CR.chMgr.FetchCommunityEpochsChannel()
        if chCE != nil {
            commEpoch = chCE.FetchCommunityEpoch(inEpochID, inLiveOnly)
        }
    }

    // If inEpochID is nil, then the genesis epoch is assumed
    if commEpoch == nil || bytes.Equal(inEpochID, CR.GenesisSeed.CommunityEpoch.EpochTID) {
        commEpoch = CR.GenesisSeed.CommunityEpoch
    }

    return commEpoch
}


func (CR *CommunityRepo) registerCommCrypto(inCommSession *CommunityCrypto) {

    CR.commSessionsMutex.Lock()
    found := false
    for _, commSession := range CR.commSessions {
        if commSession == inCommSession {
            found = true
            break
        }
    }
    if ! found {
        CR.commSessions = append(CR.commSessions, inCommSession)
    }
    CR.commSessionsMutex.Unlock()

}



func (CR *CommunityRepo) unregisterCommCrypto(inCommSession *CommunityCrypto) {

    CR.commSessionsMutex.Lock()
    N := len(CR.commSessions)
    for i := 0; i < N; i++ {
        if CR.commSessions[i] == inCommSession {
            N--
            CR.commSessions[i] = CR.commSessions[N]
            CR.commSessions = CR.commSessions[:N]
        }
    }
    CR.commSessionsMutex.Unlock()

    if N == 0 {
        CR.spSyncStop()
    }

}