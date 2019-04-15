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
    
    "github.com/plan-systems/go-plan/ski/Providers/hive"

 	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/pcore"

    "github.com/dgraph-io/badger"
)


// Config are params needed in order to run/start a CommunityRepo
// TODO: where do these settings go?  StorageEpoch 
type Config struct {

    // Max number of seconds that any two community peers could have different clock readings
    MaxPeerClockDelta       int64               `json:"max_peer_clock_delta"`

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
    flow                    plan.Flow

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

    spClient                pdi.StorageProviderClient
    spInfo                 *pdi.StorageInfo
    spCancel                context.CancelFunc
    spContext               context.Context
    spClientConn           *grpc.ClientConn

    spCommitContext         context.Context
    spCommitCancel          context.CancelFunc

  //  spScanContext           context.Context
  //  spScanCancel            context.CancelFunc

    //spCommitTxns            pdi.StorageProvider_CommitTxnsClient

    unpacker                ski.PayloadUnpacker


    syncMode                syncMode

    txnsToCommit            chan pdi.RawTxn
    txnsToDecode            chan pdi.RawTxn
    txnsToWrite             chan pdi.RawTxn

    txnsToRequest           chan *pdi.TxnList         // URIDs to be fetched from the SP (that the repo needs)
    txnCollater             pdi.TxnCollater

    txnWorklist             *badger.DB      // Status info by txn URID

    entriesToMerge          chan *entryIP
    CommunityKeyringName    []byte

    communitySKI            ski.Session
    communitySKICond        *sync.Cond
    communitySKIMutex       sync.RWMutex

    txnScanning             sync.WaitGroup

    //pipelines               sync.WaitGroup

    MemberSessions          MemberSessions

/*
    storage                 pdi.StorageSession
    storageProvider         pdi.StorageProvider
    storageMsgs             <-chan *pdi.StorageMsg

    txnsToProcess          chan txnInProcess

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





// NewCommunityRepo creates a CommunityRepo for use
func NewCommunityRepo(
    inHomePath string,
    inSeed *RepoSeed,
) (*CommunityRepo, error) {
    
    var err error

    CR := &CommunityRepo{
        //Config: inConfig,
        DefaultFileMode: plan.DefaultFileMode,
        //txnsToProcess: make(chan txnInProcess),
        unpacker: ski.NewUnpacker(true),
        chMgr: NewChMgr(inHomePath),
        HomePath: inHomePath,
    }

    CR.communitySKICond = sync.NewCond(&CR.communitySKIMutex)
 
    seedPathname := CR.GenesisSeedPathname()

    // If we're seeding this repo, write out critical files
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
        err = CR.LoadGenesisSeed(seedPathname)
    }

    if err == nil {
        CR.MemberSessions.Host = CR
        CR.CommunityKeyringName = CR.GenesisSeed.StorageEpoch.CommunityKeyringName()
    }

    return CR, err
}


func (CR *CommunityRepo) LoadGenesisSeed(inSeedPathname string) error {

    buf, err := ioutil.ReadFile(inSeedPathname)
    if err == nil { 
        var packingInfo ski.SignedPayload
        err = CR.unpacker.UnpackAndVerify(buf, &packingInfo)
        if err == nil {
            err = CR.GenesisSeed.Unmarshal(packingInfo.Header)
            if err == nil {
                if ! bytes.Equal(packingInfo.Signer.PubKey, CR.GenesisSeed.StorageEpoch.OriginKey.PubKey) {
                    err = plan.Errorf(nil, plan.VerifySignatureFailed, "%v failed to verify", inSeedPathname)
                }
            }
        }
    }
    return err
}



func (CR *CommunityRepo) flushState() error {

    buf, err := json.MarshalIndent(&CR.State, "", "\t")
    if err == nil {
        pathname := path.Join(CR.HomePath, "RepoState.json")

        err = ioutil.WriteFile(pathname, buf, plan.DefaultFileMode)
    }

    return err
}



// Startup starts this repo
func (CR *CommunityRepo) Startup(
    inCtx context.Context,
) error {

    if CR.flow.IsRunning() {
        panic("repo is already running")
    }

    err := CR.flow.Startup(
        inCtx,
        fmt.Sprintf("repo %v", CR.HomePath),
        CR.onInternalStartup,
        CR.onInternalShutdown,
    )

    return err
}

// Shutdown initiates a full shutdown of this repo, blocking until complete.
func (CR *CommunityRepo) Shutdown(inReason string) {

    CR.flow.Shutdown(inReason)

}

// GenesisSeedPathname returns the pathname of the signed genesis seed.
func (CR *CommunityRepo) GenesisSeedPathname() string {
    return path.Join(CR.HomePath, "GenesisSeed.signed")
} 


func (CR *CommunityRepo) onInternalStartup() error {
    
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

    // Create a heap-only key hive used for the community keyring
    if CR.communitySKI, err = hive.StartSession("", "", nil); err != nil {
        return err
    }

    if CR.txnDB, err = badger.Open(opts); err != nil {
        return plan.Error(err, plan.StorageNotReady, "CommunityRepo.txnDB.Open() failed")
    }

    //
    //
    //
    //
    // inbound pdi entry processor
    //
    // Processes a new pdi.EntryCrypt and dispatches it to the appropriate channel pipeline
    CR.entriesToMerge = make(chan *entryIP, 1)
    CR.flow.ShutdownComplete.Add(1)
    go func() {

        for eip := range CR.entriesToMerge {
            err := CR.decryptAndMergeEntry(eip)
            CR.flow.Log.WithError(err).Warn("entry failed to process")
        }

        CR.chMgr.flow.Shutdown(CR.flow.ShutdownReason)

        CR.flushState()

        // TODO: shutdown channel subsystem here
        {}

        CR.txnDB.Close()
        CR.txnDB = nil

        if CR.communitySKI != nil {
            CR.communitySKIMutex.Lock()
            CR.communitySKI.EndSession(CR.flow.ShutdownReason)
            CR.communitySKI = nil
            CR.communitySKIMutex.Unlock()
        }

        CR.flow.ShutdownComplete.Done()
    }()
    //
    //
    //
    //
    // txnsToWrite processor
    //
    // Writes incoming raw txns (that have been validated) to the txn DB
    CR.txnsToWrite = make(chan pdi.RawTxn, 8)
    go func() {
    
        for txn := range CR.txnsToWrite {
            dbTxn := CR.txnDB.NewTransaction(true)
            dbErr := dbTxn.Set([]byte(txn.URID), txn.Bytes)
            if dbErr == nil {
                dbErr = dbTxn.Commit()
            } else {
                dbTxn.Discard()
            }

            CR.flow.Log.Infof("stored     txn %v", ski.BinDesc(txn.URID))
            
            if dbErr != nil {
                err := plan.Errorf(dbErr, plan.TxnDBNotReady, "failed to write txn %v to db", txn.URID)
                if CR.flow.FilterFault(err) != nil {
                    break
                }
            }
        }

        close(CR.entriesToMerge)
    }()
    //
    //
    //
    //
    // txnsToDecode processor
    //
    // Decodes incoming raw txns and fans each out to:
    //    - the txn DB write queue
    //    - txn payload handling
    CR.txnsToDecode = make(chan pdi.RawTxn, 1)
    go func() {

        // TODO: choose different encoder based on spInfo
        txnDecoder := ds.NewTxnDecoder(false)

        for txnIn := range CR.txnsToDecode {

            // TODO: use sync.Pool
            seg := &pdi.DecodedTxn{
                RawTxn: txnIn.Bytes,
            }

            err := seg.DecodeRawTxn(txnDecoder)
            if err != nil {
                err = plan.Error(err, plan.TxnDecodeFailed, "txn decode failed")
            } else {
                txnIn.URID = seg.Info.URID
            }

            // TODO: (DoS security) check that the SP isn't handing back wrong/unrequested txns
            if err == nil {
                {}
            }

            if err == nil {
                CR.txnsToWrite <- txnIn
            }

            URID := seg.URID

            if err == nil {
                var solo *pdi.DecodedTxn
                solo, err = CR.txnCollater.Desegment(seg)

                if solo != nil && err == nil {
                    URID = seg.URID
                    err = CR.DispatchPayload(solo)
                }
            }

            if err != nil {
                CR.flow.Log.WithError(err).Warnf("err processing txn %v", URID)
            }
        }

        close(CR.txnsToWrite)
    }()
    //
    //
    //
    //
    // txn requester
    //
    // Dispatches txn ID requests to the community's (remote) StorageProvider(s), managing connections etc.
    CR.txnsToRequest = make(chan *pdi.TxnList, 16)
    go func() {

        /*var (
            err error
            spReader pdi.StorageProvider_SendTxnsClient
        )*/

        for txnList := range CR.txnsToRequest {
            
            // Drop requests until the channel closes.  This avoids case where the shutdown process is blocked b/c of a full channel.
            if ! CR.flow.IsRunning() {
                continue
            }

            spReader, err := CR.spClient.SendTxns(CR.spContext, txnList)
            for err == nil && CR.flow.IsRunning() {
                var txnIn *pdi.RawTxn
                txnIn, err = spReader.Recv()
                if txnIn != nil {
                    CR.txnsToDecode <- *txnIn
                }
            }
            if err != nil && err != io.EOF {
                CR.flow.Log.WithError(err).Warn("got batch err")
            }
        }

        close(CR.txnsToDecode)
    }()
    //
    //
    //
    //
    // txn committer
    //
    // Receives txns ready to be committed to the community's storage
    CR.txnsToCommit = make(chan pdi.RawTxn, 16)
    go func() {

        for txn := range CR.txnsToCommit {

            // TODO drop txn gracefully?

            if CR.spCommitContext == nil {
                CR.spCommitContext, CR.spCommitCancel = context.WithCancel(CR.spContext)
            }


            CR.flow.Log.Infof("committing txn %v", ski.BinDesc(txn.URID))


            // TODO: use stream input and output so that commit details can be reported?
            _, err := CR.spClient.CommitTxn(CR.spCommitContext, &pdi.RawTxn{Bytes: txn.Bytes})
            if err != nil {
                CR.flow.Log.WithError(err).Warn("got commit err")
            }
        }

        if CR.spCancel != nil {
            CR.spCancel()
        }

        // Scanning will stop once it sees the CR is shutting down so wait until we know for sure that scanning is done so we know no new txns are requested
        CR.txnScanning.Wait()

        close(CR.txnsToRequest)
    }()
    //
    //
    //
    //
    // txn update monitor
    /*
        var URIDs []byte

        for {

            // In the futurtre
            URIDs, err := CR.scanForTxns(URIDs)
            if err == nil && len(URIDs) > 0 {
                URIDs, err = CR.filterMissingTxns(URIDs)
            }

            if txnBatch != nil {
                
                for CR.opState == repoStarting || CR.opState == repoRunning {
                    var err error
                    txnBatch.URIDs, err = CR.filterMissingTxns(txnBatch.URIDs)
                    if CR.RetryCriticalOp(err) {
                        continue
                    }

                    // Request whatever txns we're missing
                    if len(txnBatch.URIDs) > 0 {
                        CR.txnsToRequest <- txnBatch 
                    }
                }
            }
            if recvErr != nil {
                CR.flow.Log.WithError(recvErr).Warn("scanForMissingTxns recvErr")
            }
        }

        CR.flow.Log.Debug("missingTxnScanner() exiting")
        CR.partsRunning.Done()
    

    

            if CR.spCommitTxns == nil {

                if err != nil {
                    // TODO err?
                }
                
                // Receive txn status info on another thread
                go func() {
                    for CR.CheckState() != nil {
                        metaInfo, err := CR.spCommitTxns.Recv()
                        if err != nil {
                            break
                        }
                        if metaInfo.TxnStatus == pdi.TxnStatus_COMMITTED {
                            // TODO: remove txn from worklist
                        }
                    }
                }
            }

            err = CR.spCommitTxns.Send(&pdi.RawTxn{txn.RawTxn})
            if err != nil {
                // TODO err?
            }
        }*/

    if err = CR.chMgr.Startup(CR.flow.Ctx); err != nil {
        return err
    }
    


    return nil
}


func (CR *CommunityRepo) onInternalShutdown() {

    // First, end all member sessions
    CR.MemberSessions.Shutdown("parent repo shutting down", nil)

    // This initiates a close-cascade
    if CR.txnsToCommit != nil {
        close(CR.txnsToCommit)
    }

}







/*
type StorageAgent struct {


}

func (SA *StorageAgent) New() {

}


func (SA *StorageAgent) Shutdown() {

}
*/


// DisconnectFromStorage disconnects from the currently connected StorageProvider
func (CR *CommunityRepo) DisconnectFromStorage() {

    if CR.spClientConn != nil {
        CR.spClientConn.Close()
        CR.spClientConn = nil
    }

    CR.spClient = nil
    CR.spContext = nil
    CR.spClientConn = nil
    CR.spInfo = nil
    if CR.spCancel != nil {
        CR.spCancel()
        CR.spCancel = nil
    }
}


// ConnectToStorage connects to the given storage and blocks until a fatal error or the connection is over.
func (CR *CommunityRepo) ConnectToStorage() error {

    // TODO
    // DisconnectFromStorage()

// TODO -- is there something better to use?
// communitySKICond shouldn't be used here.  Instead, it's used to inititate a repo processing txns that have been recieved but not yet processed (or
// the retry txns that referenced a community key that wasn't (yet) present. 
CR.communitySKIMutex.Lock()
CR.communitySKICond.Wait()
CR.communitySKIMutex.Unlock()


    //CR.scanMode = syncReverseScan

    var err error
	CR.spContext, CR.spCancel = context.WithCancel(CR.flow.Ctx)

    addr := CR.State.Services[0].Addr
	CR.spClientConn, err = grpc.DialContext(CR.spContext, addr, grpc.WithInsecure())
	if err != nil {
        err = plan.Errorf(err, plan.FailedToConnectStorageProvider, "grpc.Dial() failed with addr %v", addr)
	}

    if err == nil {
	    CR.spClient = pdi.NewStorageProviderClient(CR.spClientConn)
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
        if err != nil {
            err = plan.Error(err, plan.FailedToConnectStorageProvider, "StartSession() failed")
        }

        if err == nil {
            CR.spContext, err = pcore.TransferSessionToken(CR.spContext, trailer)
            if err != nil {
                err = plan.Error(err, plan.FailedToConnectStorageProvider, "TransferSessionToken() failed")
            }
        }
    }

    if err != nil {
        CR.DisconnectFromStorage()
        return err
    }

    CR.txnScanning.Add(1)
    go CR.forwardTxnScanner()

    return nil
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




type syncMode int

const (
	syncReverseScan syncMode = 0
    syncForwardScan
)


func (CR *CommunityRepo) filterNeededTxns(ioTxnList *pdi.TxnList) error {

    count := 0
    N := len(ioTxnList.URIDs)
    if N != len(ioTxnList.Statuses) {
        CR.flow.Log.Warn("received bad TxnList")
        return plan.Error(nil, plan.StorageNotConsistent, "received bad TxnList")
    }

    var err error

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
                        err = plan.Errorf(itemErr, plan.TxnDBNotReady, "error reading txn DB key %v", URID)
                        err = CR.flow.FilterFault(err)
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

    for CR.flow.IsRunning() {

        CR.flow.Log.Info("starting forward scan")

        scanCtx, scanCancel := context.WithCancel(CR.spContext)
        scanner, err := CR.spClient.Scan(
            scanCtx,
            &pdi.TxnScan{
                TimestampStart: CR.State.LastTxnTimeRead,
                TimestampStop: pdi.URIDTimestampMax,
                SendTxnUpdates: true,
            },
        )
        if err != nil {
            CR.flow.Log.WithError(err).Warn("unexpected Scan() err")
        }

        for err == nil && CR.flow.IsRunning() {
            var txnList *pdi.TxnList
            txnList, err = scanner.Recv()
        
            if ! CR.flow.IsRunning() {
                break
            } else if err != nil {
                CR.flow.Log.WithError(err).Warn("forward scan recv err")
                break
            } else if txnList != nil{

                // Filter for txn we need
                CR.filterNeededTxns(txnList)

                // Request txns we're missing
                if len(txnList.URIDs) > 0 {
                    CR.txnsToRequest <- txnList 
                }
            }
        }


        select {
            case <- scanCtx.Done():
            case <- time.After(5 * time.Second):
        }
        
        scanCancel()

    }

    CR.txnScanning.Done()
}






// DispatchPayload unpacks a decoded txn which is given to be a single/sole segment.
func (CR *CommunityRepo) DispatchPayload(txn *pdi.DecodedTxn) error {

    if txn.Info.SegTotal != 1 || txn.Info.SegIndex != 0 {
        return plan.Errorf(nil, plan.CannotExtractTxnPayload, "segments missing or txn ill-formed")
    }

    switch txn.Info.PayloadEncoding {

        case plan.Encoding_Pb_EntryCrypt:
            eip := &entryIP{}   // TODO: use sync.Pool
            err := eip.EntryCrypt.Unmarshal(txn.PayloadSeg)
            if err != nil {
                return plan.Errorf(nil, plan.CannotExtractTxnPayload, "failed to unmarshal EntryCrypt from txn payload")
            }
            eip.Entry.URID = txn.Info.PayloadName
            CR.entriesToMerge <- eip

        default:
            return plan.Errorf(nil, plan.UnsupportedPayloadCodec, "txn payload codec %v not supported", txn.Info.PayloadEncoding)
    }

    return nil
}


func (CR *CommunityRepo) decryptAndMergeEntry(eip *entryIP) error {

    eip.timeStart = plan.Now()
    

    // STEP 1 -- Decrypt the entry header and body using the cited community key ID.
    //var decryptOut *ski.CryptOpOut 
    decryptOut, err := CR.communitySKI.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_DECRYPT_SYM,
        OpKey: &ski.KeyRef{
            KeyringName: CR.CommunityKeyringName,
            PubKey: eip.EntryCrypt.CommunityPubKey,
        },
        BufIn: eip.EntryCrypt.PackedEntry,
    })
    if plan.IsError(err, plan.KeyringNotFound, plan.KeyEntryNotFound) {
        // TODO: append txn URID onto a list for later processing
        // Status info stored w/ txns:
        //    - time received, txn status (revoked?), attempts to merge, merge fail reasons/history
    }
    if err != nil {
        return err
    }

    var packingInfo ski.SignedPayload
    err = CR.unpacker.UnpackAndVerify(
        decryptOut.BufOut,
        &packingInfo,
    )
    if err != nil {
        return err
    }

    eip.Entry.Body = packingInfo.Body

    err = eip.Entry.Info.Unmarshal(packingInfo.Header)
    if err != nil {
        return err
    }

    var scrap [pdi.URIDBinarySz]byte
    actualURID := pdi.URIDFromInfo(scrap[:], eip.Entry.Info.TimeAuthored, packingInfo.Hash)
    if ! bytes.Equal(eip.Entry.URID, actualURID) {
        return plan.Errorf(err, plan.TxnNotConsistent, "txn payload URID was %v but actual is %v", eip.Entry.URID, actualURID)
    }

    /* TODO: perform timestamp sanity checks
    if eip.EntryInfo.TimeAuthored < eip.CR.Info.TimeCreated.UnixSecs {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp earlier than community creation timestamp")
    }
    if eip.timeStart.UnixSecs - eip.entryHeader.TimeAuthored + eip.CR.Info.MaxPeerClockDelta < 0 {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp too far in the future")
    }*/

    /*
    Every entry has dependencies:
        - the member epoch that contains the member's signing pub key (member reg channel, entry ID, time index)
        - */

    eip.Entry.ChEntry.AssignFromDecrytedEntry(&eip.Entry.Info, packingInfo.Signer.PubKey)

    // Verify that the entry is actually a genesis entry
    if eip.Entry.Info.IsGenesisEntry {
        found := false
        for _, URID := range CR.GenesisSeed.StorageEpoch.GenesisURIDs {
            if bytes.Equal(URID, eip.Entry.URID) {
                eip.Entry.ChEntry.AddFlags(ChEntryFlag_IS_GENESIS_ENTRY)
                found = true
                break
            }
        }
        if ! found {
            eip.Entry.ChEntry.ThrowMalformed(
                plan.Errorf(nil, plan.GenesisEntryNotVerified, "genesis entry %v not found", eip.Entry.URID),
            )
        }
    }


    CR.chMgr.MergeEntry(&eip.Entry)
 
    // At this point, the PDI entry's signature has been verified
    return err

}


// StartMemberSession starts a new session for the given member
func (CR *CommunityRepo) StartMemberSession(in *SessionReq) (*MemberSession, error) {
    if ! bytes.Equal(in.CommunityID, CR.GenesisSeed.StorageEpoch.CommunityID) {
        return nil, plan.Error(nil, plan.AssertFailed, "community ID does not match repo's ID")
    }

   // TODO: make repo ski sesson mgr?  use workstation path??  
    skiDir, err := hive.GetSharedKeyDir()
    if err != nil { return nil, err }

    // TODO: close prev skiSession
    // TODO: load pw file w/ pw hash, etc
    personalSKI, err := hive.StartSession(
        skiDir,
        in.MemberEpoch.FormMemberStrID(),
        nil,
    )
    if err != nil { return nil, err }

    ms, err := CR.MemberSessions.StartSession(
        in,
        personalSKI,
        CR.HomePath,
    )
    if err != nil {
        return nil, err
    }

    tomeBuf, pw := ms.ExportCommunityKeyring()
    _, err = CR.communitySKI.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_IMPORT_USING_PW,
        BufIn: tomeBuf,
        PeerKey: pw,
    })
    if err == nil {
        CR.communitySKICond.Broadcast()
    }

    CR.flow.LogErr(err, "error importing community keyring")

    return ms, nil
}


/*****************************************************
** MemberHost (interface)
**/

// Context -- see interface MemberHost
func (CR *CommunityRepo) Context() context.Context {
    return CR.flow.Ctx
}

// CommitEntryTxns -- see interface MemberHost
func (CR *CommunityRepo) CommitEntryTxns(inEntryURID []byte, inTxns []pdi.RawTxn) {
    for _, txn := range inTxns {
        CR.txnsToCommit <- txn
        CR.txnsToWrite  <- txn
    }

}

// LatestCommunityEpoch -- see interface MemberHost
func (CR *CommunityRepo) LatestCommunityEpoch() pdi.CommunityEpoch {
    return *CR.GenesisSeed.CommunityEpoch       // TODO: fix me when community epochs are implemented
}

// LatestStorageEpoch -- see interface MemberHost
func (CR *CommunityRepo) LatestStorageEpoch() pdi.StorageEpoch {
    return *CR.GenesisSeed.StorageEpoch
}

// OnSessionEnded -- see interface MemberHost
func (CR *CommunityRepo) OnSessionEnded(inSession *MemberSession) {
    CR.MemberSessions.OnSessionEnded(inSession)
}



type DecryptedEntry struct {
    Info            pdi.EntryInfo
    URID            []byte
    Body            []byte
    ChEntry         ChEntryInfo
    ChAgentAsset    []byte
}

// ChannelID returns this entry's destination channnel ID.
func (entry *DecryptedEntry) ChannelID() plan.ChannelID {
    return plan.ChannelID(entry.Info.ChannelID)
}


type entryIP struct {

    timeStart       plan.Time

    // Txn ID of the last/final segment storage provider txn
   // ParentURID      string

/*
    entryTxnIndex   int
    parentTxnName   []byte

    txnWS           []txnWorkspace
    entryBatch      []*pdi.EntryCrypt // 
    entryIndex      int               // This is the index number into entryBatch that is currently being processed
    entryTxn        pdi.StorageTxn */

    Entry           DecryptedEntry

    EntryCrypt      pdi.EntryCrypt


    //ChannelEpoch    *pdi.ChannelEpoch
    
 //   authorEpoch     *pdi.MemberEpoch

 //   skiSession      ski.Session

/*
    skiProvider     ski.Provider

    accessCh        *ChannelStore
    accessChFlags   LoadChannelStoreFlags

    targetCh        *ChannelStore
    targetChFlags   LoadChannelStoreFlags*/

}




/*
type entryInProcess struct {
    CR              *CommunityRepo    

    timeStart       plan.Time


    // Txn ID of the last/final segment storage provider txn
    ParentURID      string


    entryTxnIndex   int
    parentTxnName   []byte

    txnWS           []txnWorkspace
    entryBatch      []*pdi.EntryCrypt // 
    entryIndex      int               // This is the index number into entryBatch that is currently being processed
    entryTxn        pdi.StorageTxn 

    entryHash       []byte
    EntryInfo     pdi.EntryInfo
    EntryCrypt      pdi.EntryCrypt
    entryBody       plan.Block
    
    authorEpoch     pdi.MemberEpoch

    skiSession      ski.Session


    skiProvider     ski.Provider

    accessCh        *ChannelStore
    accessChFlags   LoadChannelStoreFlags

    targetCh        *ChannelStore
    targetChFlags   LoadChannelStoreFlags

}



type EpochID uint64


type Channel interface {
    LookupEpoch(inEpochID EpochID) (*pdi.ChannelEpoch, error)
}



type AccessChannel interface {


}



type GeneralChannel interface {


}


func (eip *entryInProcess) prepChannelAccess() error {

    plan.Assert( eip.targetChFlags == 0 &&  eip.accessChFlags == 0, "channel store lock flags not reset" )

    targetChFlags := LockForWriteAccess
    accessChFlags := LockForReadAccess | CitedAsAccessChannel

    switch eip.EntryInfo.EntryOp {
        case pdi.EntryOp_EDIT_ACCESS_GRANTS:
            targetChFlags |= CitedAsAccessChannel
    }

    // First lock the target channel
    var err error
    eip.targetCh, err = eip.CR.LockChannelStore(eip.entryHeader.ChannelId, targetChFlags)
    if err != nil {
        return err
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    eip.targetChFlags = targetChFlags

    // Step from newest to oldest epoch.
    var epochMatch *pdi.ChannelEpoch
    for i, epoch := range eip.targetCh.ChannelEpochs {
        if epoch.EpochId == eip.entryHeader.ChannelEpochId {
            if i > 0 {
                // TODO: ws.targetChannel.ChannelEpoch[i-1].EpochTransitionPeriod
                {

                    // TargetChannelEpochExpired
                }
            }
        }
    }
    if epochMatch == nil {
        return plan.Errorf(nil, plan.TargetChannelEpochNotFound, "epoch 0x%x for target channel 0x%x not found", eip.entryHeader.ChannelEpochId, eip.entryHeader.ChannelId)
    }

    // Lookup the latest 
    eip.accessCh, err = eip.CR.LockChannelStore(epochMatch.AccessChannelId, accessChFlags)
    if prr != nil {
        return err
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    eip.accessChFlags = accessChFlags


    // Ops such as REMOVE_ENTRIES and SUPERCEDE_ENTRY
    perr = ws.targetCh.FetchRelevantEntriesForOp()

    access := ws.accessCh.LookupAccessForAuthor(ws.entryHeader.AuthorMemberId)

    reqs := entryAccessReqs{

    }
    switch ws.entryHeader.EntryOp {
    case POST_NEW_CONTENT:
        reqs.minAccessLevel = READWRITE_ACCESS
        case pdi.EntryOp_EDIT_ACCESS_GRANTS:
            targetChFlags |= CitedAsAccessChannel
    }
*/


/*
      // Fetch and lock the data container for the cited access channel, checking all security permissions
    ws.targetChannel, err = ws.CR.LockChannelStoreForOp(ws.entryHeader)
    if err != nil {
        return err
    }

    accessLevel, err := ws.targetChannel.AccessLevelForMember(ws.entryHeader.
    var 
    for i, chEpoch := range ws.targetChannel.ChannelEpochs {
        if chEpoch.EpochId == ws.entryHeader.ChannelEpochId {
            for 
        }
    }

    fetchFlags := LockForReadAccess

    switch ( ws.entryHeader.EntryOp ) {

        case 
            EntryOp_UPDATE_ACCESS_GRANTS,
            EntryOp_EDIT_CHANNEL_EPOCH:
            
            fetchFlags = LockForWriteAccess
    }

    fetchFlags |= IsAccessChannel
    ws.targetChannel, err = ws.CR.FetchChannelStore(
        ws.targetChannel.ChannelEpoch.AccessChannelId, 
        ws.entryHeader.ChannelEpoch,
        fetchFlags )
    

    CitedAsAccessChannel

/*
    if ws.targetChannel == nil {
        return plan.Errorf(err, plan.AccessChannelNotFound, "channel 0x%x not found", ws.entryHeader.ChannelId )
    }

    if ws.targetChannel.ACStore == nil {
        return plan.Errorf(nil, plan.NotAnAccessChannel, "invalid channel 0x%x", ws.entryHeader.ChannelId )
    }

    // TODO: do all of ACStore checking!
*/
  



    /*
    // TODO: choose different encoder based on spInfo
    decoder := ds.NewTxnDecoder()

    resumeReadingFrom := int64(0)

    CR.flow.Log.Infof("Seeking to time %v", resumeReadingFrom)

    spQuery, qerr := CR.spClient.Query(
        CR.spContext,
        &pdi.TxnQuery{
            TimestampMin: resumeReadingFrom,
            TimestampMax: plan.DistantFuture,
        },
        nil,
    )

    if qerr != nil {
        // TODO??
    }

    for {
        txnBundle, connErr := spQuery.Recv()
        if connErr != nil {
            break
        }

        txnCount := 0 
        for _, txn := range txnBundle.Txns {
            txnCount++
            
            var txnInfo pdi.TxnInfo
            var txnSeg pdi.TxnSegment

            CR.flow.Log.Infof("Received txn %v", txn.URID)
            err = decoder.DecodeRawTxn(
                txn.RawTxn,
                &txnInfo,
                &txnSeg,
            )
            if err != nil {
                log.Fatal(err)
            }
            n := txnReplayStart + txnCount -1
            if bytes.Compare(txnSeg.SegData, testPayloads[n]) == 0 {
                log.Infof("%d of %d checked!", txnCount, txnsToExpectBack)
            } else {
                log.Fatalf("failed check #%d", i)
            }
        }


                conn, connErr := pn.SP.Query(ctx, &pdi.TxnQuery{
                    TimestampMin: queryStartTime,
                    TimestampMax: queryStartTime + int64(txnsToExpectBack),
                })

                for i := 0; connErr == nil; i++ {
                    txnBundle, connErr := conn.Recv()
                    if connErr != nil {      // io.EOF
                        break
                    }

                    txnCount := 0 
                    for _, txn := range txnBundle.Txns {
                        txnCount++
                        
                        var txnInfo pdi.TxnInfo
                        var txnSeg pdi.TxnSegment

                        log.Infof("Recieved txn URID %v", txn.URID)
                        err = decoder.DecodeRawTxn(
                            txn.RawTxn,
                            &txnInfo,
                            &txnSeg,
                        )
                        if err != nil {
                            log.Fatal(err)
                        }
                        n := txnReplayStart + txnCount -1
                        if bytes.Compare(txnSeg.SegData, testPayloads[n]) == 0 {
                            log.Infof("%d of %d checked!", txnCount, txnsToExpectBack)
                        } else {
                            log.Fatalf("failed check #%d", i)
                        }
                    }
                }

    */
