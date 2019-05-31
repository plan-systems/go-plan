package repo


import (
    "bytes"
    "log"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    "context"
    "fmt"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"
    //"encoding/binary"
    "sync/atomic"

    //"github.com/plan-systems/go-plan/pcore"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    //"github.com/plan-systems/go-plan/pservice"
    //"github.com/plan-systems/go-plan/ski"


    "github.com/dgraph-io/badger"

)


// ChMgr is the top level interface for a community's channels.
type ChMgr struct {
    flow                    plan.Flow

    HomePath                string

    nextSessionID           uint32
    StorageEpoch            pdi.StorageEpoch               
    CR                      *CommunityRepo

    //db                      *badger.DB
    //State                   ChMgrState

    //fsNameEncoding          *base64.Encoding


   // revalQueue              chan revalidateMsg
    chMutex                 sync.RWMutex
    chLoaded                map[plan.ChIDBlob]ChAgent

}



func NewChMgr(
    inHomeDir string,
    CR *CommunityRepo,
) *ChMgr {

    chMgr := &ChMgr{
        chLoaded: make(map[plan.ChIDBlob]ChAgent),
        HomePath: path.Join(inHomeDir, "ChMgr"),
        nextSessionID: 0,
        StorageEpoch: *CR.GenesisSeed.StorageEpoch,
        CR: CR,
        //fsNameEncoding: plan.Base64
    }

    return chMgr

}

// Startup -- see plan.Flow.Startup()
func (chMgr *ChMgr) Startup(
    inCtx context.Context,
) error {

    err := chMgr.flow.Startup(
        inCtx,
        "chMgr",
        chMgr.onInternalStartup,
        chMgr.onInternalShutdown,
    )

    return err
}



func (chMgr *ChMgr) onInternalStartup() error {

    /*
    opts := badger.DefaultOptions
    opts.Dir = path.Join(chMgr.HomePath, "chMgr.badger")
    opts.ValueDir = opts.Dir

    var err error
    if chMgr.db, err = badger.Open(opts); err != nil {
        return plan.Error(err, plan.StorageNotReady, "chMgr db failure")
    }

    // Load latest chMgr info
    if err == nil {
        dbTxn := chMgr.db.NewTransaction(false)

        var item *badger.Item

        item, err = dbTxn.Get(chMgrStateKey)
        if err == nil {
            err = item.Value(func(val []byte) error {
                return chMgr.State.Unmarshal(val)
            })
        } else if err == badger.ErrKeyNotFound {
            chMgr.State = ChMgrState{}
            err = nil
        }

        dbTxn.Discard()
    }*/

    return nil
}
  

func (chMgr *ChMgr) onInternalShutdown() {

    // The assumption at this point is that no new entries are being sent to channels to be merged.
    // This means that we can close each channel's entriesToMerge to proceed with shutdown
    var waitingOn []*ChStore
    for {

        shutdown := &sync.WaitGroup{} 

        // First, cause all the ChStores to exit their main entry processing loop.
        chMgr.chMutex.RLock()
        {
            // Make a list to hold the channels we're waiting on
            N := len(chMgr.chLoaded)
            if cap(waitingOn) < N {
                waitingOn = make([]*ChStore, 0, N)
            } else {
                waitingOn = waitingOn[:0]
            }
            shutdown.Add(N)
            for _, ch := range chMgr.chLoaded {
                chSt := ch.Store()
                waitingOn = append(waitingOn, chSt)
                go func() {
                    chSt.ShutdownEntryProcessing(true)
                    shutdown.Done()
                }()
            }
        }
        chMgr.chMutex.RUnlock()

        if len(waitingOn) == 0 {
            break
        }

        chMgr.CR.Infof(1, "waiting on %d channels to shutdown", len(waitingOn))

        // Wait until every loaded channel has exited it main validation loop
        shutdown.Wait()

        // At this point, nothing is running (so no futher calls to FetchChannel() should/will be made), so now we can fully shutdown each channel.
        shutdown.Add(len(waitingOn))
        for _, chSt := range waitingOn {
            chMgr.detachChannel(chSt.ChID())
            go chSt.Shutdown(shutdown)
        }

        // Wait until all channels have fully shutdown (dbs closed, etc)
        shutdown.Wait()
    }


/*{

}
func (chMgr *ChMgr) rebootChannel(
    inChID plan.ChID,
    inChEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    chID := inChID.Blob()

    chMgr.chMutex.Lock()
    ch := chMgr.chLoaded[chID]
    delete(chMgr.chLoaded, chID)
    chMgr.chMutex.Unlock()

    if ch != nil {
        shutdown := sync.WaitGroup{}

        shutdown.Add(1)
        ch.Store().Shutdown(&shutdown)

        shutdown.Wait()
    }

    return chMgr.FetchChannel(inChID, inChEpoch)
*/


}


/*
var eipPool = sync.Pool{
    New: func() interface{} {
		return new(txnSet)
	},
}


func entryIPAlloc() *entryIP {
    eip := eipPool.Get().(*entryIP)

    eip.txnsAuthored = false
    
    eip.EntryCrypt.CommunityEpoch   = eip.EntryCrypt.CommunityEpoch[:0]
    eip.EntryCrypt.PackedEntry      = eip.EntryCrypt.PackedEntry[:0]

    eip.EntryInfo.EntryOp = 0
    eip.EntryInfo.EntrySubOp = 0
    eip.EntryInfo.ChannelID = eip.EntryInfo.ChannelID[:0]
    eip.EntryInfo.SupersedesEntryID = eip.EntryInfo.SupersedesEntryID[:0]
    eip.EntryInfo.Extensions = nil

    return entry
}

*/
/*


type chEntryFlags int32
const (

	writeEntry  chEntryFlags = 1 << iota
    writeEntryState  
)
*/

type chEntryOrigin int32
const (
    entryUseAnonymous chEntryOrigin = iota
	entryFromStorageProvider
    entryRevalidating
    entryWasAuthored 
)



var entryPool = sync.Pool{
	New: func() interface{} {
		return new(chEntry)
	},
}

func chEntryAlloc(inOrigin chEntryOrigin) *chEntry {
    entry := entryPool.Get().(*chEntry)
    entry.Reuse()

    entry.origin = inOrigin

    return entry
}


type chEntryPartStatus int32
const (
    partNotLoaded chEntryPartStatus = iota
	partLoaded
    partTouched
)


type chEntry struct {
    Body                []byte
    
    Info                ChEntryInfo
    State               ChEntryState
    StatePrev           ChEntryState

    PayloadTxns         *pdi.PayloadTxns

    origin              chEntryOrigin
    scrap               []byte
    
    //Asset               pcore.Marshaller
    //newChEpoch          *pdi.ChannelEpoch
    stateStatus          chEntryPartStatus
    bodyStatus           chEntryPartStatus

    onMergeComplete     func(entry *chEntry, ch ChAgent, inErr error)

    // Used for packing and unpacking (not used for validation)
    tmpCrypt            pdi.EntryCrypt
    tmpInfo             pdi.EntryInfo


}



func (entry *chEntry) Reuse() {

    entry.Info.EntrySubOp = 0
    entry.Info.SupersedesEntryID = entry.Info.SupersedesEntryID[:0]
    entry.Info.Extensions = nil
    entry.Body = entry.Body[:0]
    entry.State.LiveIDs = entry.State.LiveIDs[:0]
    entry.onMergeComplete = nil
}




func (entry *chEntry) LivenessChanged() bool {
    return (entry.State.Status == ChEntryStatus_LIVE) != (entry.StatePrev.Status == ChEntryStatus_LIVE)
}

func (entry *chEntry) IsLive() bool {
    return entry.State.Status == ChEntryStatus_LIVE
}


func (entry *chEntry) AssignFromDecrytedEntry(
    inEntryInfo *pdi.EntryInfo,
    inAuthorSig []byte,
) {

    entry.Info.EntryOp              = inEntryInfo.EntryOp
    entry.Info.EntrySubOp           = inEntryInfo.EntrySubOp
    entry.Info.TIDs                 = append(entry.Info.TIDs[:0], inEntryInfo.TIDs...)
    entry.Info.AuthorMemberID       = 0
    entry.Info.SupersedesEntryID    = append(entry.Info.SupersedesEntryID[:0], inEntryInfo.SupersedesEntryID...)
    entry.Info.AuthorSig            = append(entry.Info.AuthorSig[:0], inAuthorSig...)
    entry.Info.Extensions           = inEntryInfo.Extensions

    entry.State.Status              = ChEntryStatus_AWAITING_MERGE
    entry.State.Flags               = 1 << byte(ChEntryFlag_WELL_FORMED)
    entry.State.LiveIDs             = entry.State.LiveIDs[:0]

    entry.StatePrev                 = entry.State
    entry.stateStatus               = partTouched
    entry.bodyStatus                = partTouched

}



// MissingFlag returns true if the given flag is NOT set
func (entry *chEntry) MissingFlag(inFlag ChEntryFlag) bool {
    flag := uint32(1) << byte(inFlag)
    return (entry.State.Flags & flag) == 0
}

// ClearFlag clears the given flag
func (entry *chEntry) ClearFlag(inFlag ChEntryFlag) {
    flag := uint32(1) << byte(inFlag)
    if entry.State.Flags & flag != 0 {
        entry.State.Flags ^= flag
        entry.TouchState()
    }
}

// HasFlag returns true if the given flag is set
func (entry *chEntry) HasFlag(inFlag ChEntryFlag) bool {
    flag := uint32(1) << byte(inFlag)
    return (entry.State.Flags & flag) == flag
}

// HasFlags returns true if ALL of the passed flags are present
func (entry *chEntry) HasFlags(inFlags...ChEntryFlag) bool {

    chFlags := entry.State.Flags

    for _, flag := range inFlags {
        bit := uint32(1) << uint8(flag)
        if (chFlags & bit) != bit {
            return false
        }
    }

    return true
}

// AddFlags ORs the given flags to this
func (entry *chEntry) AddFlags(inFlags...ChEntryFlag) {

    chFlags := entry.State.Flags

    for _, flag := range inFlags {
        bit := uint32(1) << uint8(flag)
        chFlags |= bit
    }

    if entry.State.Flags != chFlags {
        entry.State.Flags = chFlags
        entry.TouchState()
    }
}

// ThrowMalformed marks this entry as disbarred due to not being self-consistent for the given reason.
func (entry *chEntry) ThrowMalformed(inErr error) {
    entry.ClearFlag(ChEntryFlag_WELL_FORMED)
    entry.SetStatus(ChEntryStatus_DISBARRED)
}

// SetStatus sets this ch entry status to the given value
func (entry *chEntry) SetStatus(inStatus ChEntryStatus) {
    if entry.State.Status != inStatus {
        entry.State.Status = inStatus
        entry.TouchState()
    }
}

/*
// Defer marks this entry as deferred for the given reason/error but never overwrites a ChEntryStatus_DISBARRED status.
func (entry *chEntry) Defer(inErr error) {
    if ntry.State.Status != ChEntryStatus_DISBARRED
    entry.SetStatus(ChEntryStatus_DEFERRED)
}*/

// TouchState marks for entry.State to be flushed to the channel db
func (entry *chEntry) TouchState() {
    entry.stateStatus = partTouched
}

// IsWellFormed returns true if ChEntryFlag_WELL_FORMED is set for this entry.
func (entry *chEntry) IsWellFormed() bool {
    return (entry.State.Flags & (1 << byte(ChEntryFlag_WELL_FORMED))) != 0
}
// GetTID returns a slice to requested EntryTID
func (entryInfo *ChEntryInfo) GetTID(inID pdi.EntryTID) plan.TID {
    pos := inID*plan.TIDSz
    return entryInfo.TIDs[pos:pos+plan.TIDSz]
}

// EntryID returns this entry's TID.
func (entryInfo *ChEntryInfo) EntryID() plan.TID {
    if len(entryInfo.TIDs) >= plan.TIDSz {
        return entryInfo.GetTID(pdi.EntryTID_EntryID)
    } else {
        return nil
    }
}

// ChannelEpochID returns the entry TID bearing the channel epoch that authorizes this entry
func (entryInfo *ChEntryInfo) ChannelEpochID() plan.TID {
    return entryInfo.GetTID(pdi.EntryTID_ChannelEpochEntryID)
}

// AuthorEntryID returns the entry ID in the member registry channel containing the MemberEpoch that verifies this entry's author sig.
func (entryInfo *ChEntryInfo) AuthorEntryID() plan.TID {
    return entryInfo.GetTID(pdi.EntryTID_AuthorEntryID)
}

// ACCEntryID returns the ACC entry TD that authorizes this entry
func (entryInfo *ChEntryInfo) ACCEntryID() plan.TID {
    return entryInfo.GetTID(pdi.EntryTID_ACCEntryID)
}

// TimeAuthoredFS returns a unix timestamp (in 1<<16 seconds) of when is entry was authored.
func (entryInfo *ChEntryInfo) TimeAuthoredFS() plan.TimeFS {
    return entryInfo.EntryID().ExtractTimeFS()
}

// TimeAuthored returns a unix timestamp (in seconds) of when is entry was authored.
func (entryInfo *ChEntryInfo) TimeAuthored() int64 {
    return entryInfo.EntryID().ExtractTime()
}





// QueueEntryForMerge is called when an entry arrives to a repo and must be merged into the repo.
func (chMgr *ChMgr) QueueEntryForMerge(
    entry *chEntry,
) {

    var (
        ch ChAgent
        chGenesisEpoch *pdi.ChannelEpoch
        err error
    )

    chID := plan.ChID(entry.tmpInfo.ChannelID)

    switch entry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH: {
            chEpoch := &pdi.ChannelEpoch{}
            if err = chEpoch.Unmarshal(entry.Body); err != nil {
                err = plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling NEW_CHANNEL_EPOCH")
                entry.ThrowMalformed(err)
            } else {
                chEpoch.EpochTID = entry.Info.EntryID()
                chEpoch.CommunityEpochID = entry.tmpCrypt.CommunityEpochID

                // Having set critical fields, remarshal the body
                // TODO: use scrap
                entry.Body, _ = chEpoch.Marshal()
    
                // Is the the channel genesis epoch?  If so, the channel ID derives from the channel ID.
                if chEpoch.IsChannelGenesis() {
                    chID = chEpoch.EpochTID[plan.TIDSz - plan.ChIDSz:]
                    chGenesisEpoch = chEpoch
                    entry.AddFlags(ChEntryFlag_IS_CHANNEL_GENESIS) 
                }
            }
        }

        default:
            if len(chID) != plan.ChIDSz {
                err = plan.Error(err, plan.ChEntryIsMalformed, "bad channel ID len")
                entry.ThrowMalformed(err)
            }
    }

    if entry.IsWellFormed() {
        ch, err = chMgr.fetchChannel(chID, true, chGenesisEpoch)

        // Is this the channel genesis entry but the channel has ChUnknown?   This occurs when
        //    entries are merged into a channel before the channel's genesis entry arrives.
        if chGenesisEpoch != nil {
            chUnknown, _ := ch.(*ChUnknown)
            if chUnknown != nil {
                chUnk := ch.Store()

                chNew := chMgr.NewChAgent(
                    &ChStoreState{
                        ChProtocol: chGenesisEpoch.ChProtocol,
                        ChannelID: chID,
                    }, 
                    false,
                )

                if chNew == nil {
                    chUnk.Warnf("channel protocol %v not recognized", chGenesisEpoch.ChProtocol)
                } else {

                    // Stop entry processing for the ChUnknown we want to take off service
                    chUnk.Info(1, "rebooting channel")
                    chUnk.ShutdownEntryProcessing(true)

                    // Hot swap the ChUnknown with the new agent
                    chMgr.chMutex.Lock()
                    {
                        chSt := chNew.Store()

                        chSt.db = chUnk.db
                        chUnk.db = nil
                        
                        // When switching to a real 
                        chSt.State.ValidatedUpto = nil

                        chMgr.chLoaded[chID.Blob()] = chNew
                    }
                    chMgr.chMutex.Unlock()

                    ch = chNew
                    err = startupChannel(chNew)
                }
            }
        }
    }


    if ch == nil {
        //chEntry.Status = ChEntryStatus_DEFERRED
        panic("TODO")
    } else {
        ch.Store().entriesToMerge <- entry
    }


    //chMgr.Log(entryErr, "error merging entry")
    //chMgr.Log(processErr, "error processing entry") */
}






func (chMgr *ChMgr) Log(inErr error, inMsg string) {
    if inErr != nil {
        chMgr.flow.LogErr(inErr, inMsg)
    }
}

// FetchACC returns the given channel known/presumed to be an ACC.
func (chMgr *ChMgr) FetchACC(
    inChID plan.ChID,
) (ChAgent, error) {

    ch, err := chMgr.ChannelExists(inChID)

    /*
    if ch != nil {
        acc, _ := ch.(*ChACC)
        if acc == nil {
            err = plan.Errorf(nil, plan.NotAnACC, "cited channel %v is not an access control channel", inChID)
        }
    }*/

    if err != nil {
        return nil, err
    }

    return ch, nil
}


// FetchMemberRegistry returns the community's member registry channel
func (chMgr *ChMgr) FetchMemberRegistry() (*ChMemberRegistry, error) {

    ch, err := chMgr.FetchChannel(chMgr.StorageEpoch.MemberRegistry())
    if err != nil {
        return nil, err
    }

    memberRegistry := ch.(*ChMemberRegistry)

    return memberRegistry, nil
}


// FetchCommunityEpochsChannel returns the community's epoch channel
func (chMgr *ChMgr) FetchCommunityEpochsChannel() *ChCommunityEpochs {

    ch, err := chMgr.FetchChannel(chMgr.StorageEpoch.CommunityEpochs())
    if err != nil {
        return nil
    }

    chCE, _ := ch.(*ChCommunityEpochs)

    return chCE
}



/*
func (chMgr *ChMgr) rebootChannel(
    inChID plan.ChID,
    inChEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    chID := inChID.Blob()

    chMgr.chMutex.Lock()
    ch := chMgr.chLoaded[chID]
    delete(chMgr.chLoaded, chID)
    chMgr.chMutex.Unlock()

    if ch != nil {
        shutdown := sync.WaitGroup{}

        shutdown.Add(1)
        ch.Store().Shutdown(&shutdown)

        shutdown.Wait()
    }

    return chMgr.FetchChannel(inChID, inChEpoch)
}
*/

// ChannelExists returns the given channel, loading it if necessary.  If the channel is not found, then it isn not autho
func (chMgr *ChMgr) ChannelExists(
    inChID plan.ChID,
) (ChAgent, error) {

    return chMgr.fetchChannel(inChID, false, nil)
}


// FetchChannel returns the owning ChAgent for the given channel ID.
// If inGenesisEpoch is set, channel genesis is performed. 
func (chMgr *ChMgr) FetchChannel(
    inChID plan.ChID,
) (ChAgent, error) {

    return chMgr.fetchChannel(inChID, true, nil)
}


// FetchChannel returns the owning ChAgent for the given channel ID.
// If inGenesisEpoch is set, channel genesis is performed. 
func (chMgr *ChMgr) fetchChannel(
    inChID plan.ChID,
    inAutoCreate bool,
    inChGenesis *pdi.ChannelEpoch,
) (ChAgent, error) {

    var (
        err error
    )

    chID := inChID.Blob()

    chMgr.chMutex.RLock()
    ch := chMgr.chLoaded[chID]
    chMgr.chMutex.RUnlock()


    if ch != nil {
        return ch, nil
    }

    if ! chMgr.flow.IsRunning() {
        return nil, plan.Error(nil, plan.ServiceShutdown, "chMgr is shutting down")
    }

    chMgr.chMutex.Lock()
    defer chMgr.chMutex.Unlock()
    
    // Recheck to handle race condition
    ch = chMgr.chLoaded[chID]

    // If the ch store isn't already loaded, try to load it.
    if ch == nil { 
        ch, err = chMgr.loadChannel(inChID, inAutoCreate, inChGenesis)

        // We've already mutexed for this write condition (above)
        if err == nil {
            chMgr.chLoaded[chID] = ch
        }  
    }
    
    // TODO: rebuild channel if db load fails?
    if err != nil {
        ch = nil
    }

    return ch, err
}



func (chMgr *ChMgr) detachChannel(
    inChID plan.ChID,
) {

    chID := inChID.Blob()

    chMgr.chMutex.Lock()
    ch := chMgr.chLoaded[chID]
    if ch != nil {
        delete(chMgr.chLoaded, chID)
    }
    chMgr.chMutex.Unlock()

}


func (chMgr *ChMgr) loadChannel(
    inChID plan.ChID,
    inAutoCreate bool,
    inChGenesis *pdi.ChannelEpoch,
) (ChAgent, error) {

    opts := badger.DefaultOptions
    opts.Dir = path.Join(
        chMgr.HomePath,
        inChID.Str(),
    )
    opts.ValueDir = opts.Dir

    if ! inAutoCreate {
        if _, err := os.Stat(opts.Dir); os.IsNotExist(err) {
            return nil, plan.Error(nil, plan.ChannelNotFound, "channel not found")
        }
    }

    if err := os.MkdirAll(opts.Dir, plan.DefaultFileMode); err != nil {
        return nil, plan.Error(err, plan.FailedToLoadChannel, "failed to create channel dir")
    }

    chDb, err := badger.Open(opts)
    if err != nil {
        err = plan.Error(err, plan.FailedToLoadChannel, "failed to open channel db")
    }

    var (
        ch ChAgent
        chState ChStoreState
    )

    // First load the latest channel state object from the chDB
    if err == nil {
        chTxn := chDb.NewTransaction(false)

        var item *badger.Item

        item, dbErr := chTxn.Get(chStateKey)
        if dbErr == nil {
            err = item.Value(func(val []byte) error {
                return chState.Unmarshal(val)
            })
            if err == nil {
                if ! bytes.Equal(chState.ChannelID, inChID) {
                    err = plan.Errorf(nil, plan.FailedToLoadChannel, "channel ID in state does not match %v, got %v", inChID, chState.ChannelID)
                }
            }
        } else if dbErr == badger.ErrKeyNotFound {
            chState.ChannelID = inChID
        }

        chTxn.Discard()
    }
    

    if err == nil {

        if len(chState.ChProtocol) == 0 && inChGenesis != nil {
            chState.ChProtocol = inChGenesis.ChProtocol
        }

        ch = chMgr.NewChAgent(&chState, true)

        chSt := ch.Store()
        chSt.db = chDb
        
        err = startupChannel(ch)
    }

    if err != nil {
        panic("TODO")
        ch = nil
    }

    return ch, err
}



// NewChAgent instantiates a new ChAgent from a channel protocol string.
//
// If inAutoCreate is set and the protocol string was not recognized, then an ChUnknown is created.
func (chMgr *ChMgr) NewChAgent(
    inChState *ChStoreState,
    inAutoCreate bool,
) ChAgent {
    var ch ChAgent

    agentFactory := gChAgentRegistry[inChState.ChProtocol]
    if agentFactory != nil {
        ch = agentFactory(inChState.ChProtocol)
    }

    mergeEnabled := true

    if ch == nil && inAutoCreate {
        ch = &ChUnknown{}
        mergeEnabled = false
    }

    if ch != nil {
        chSt := ch.Store()

        chSt.chMgr = chMgr
        chSt.entriesToMerge = make(chan *chEntry, 2)

        // If we're loading the channel and discover that merge has never been enabled, reset the validation bookmark.
        if ! chSt.State.MergeEnabled && mergeEnabled {
            chSt.State.ValidatedUpto = nil
        }
        
        chSt.State.MergeEnabled = mergeEnabled
        chSt.State.ChProtocol = inChState.ChProtocol
        chSt.State.ChannelID = append(chSt.State.ChannelID[:0], inChState.ChannelID...)
        chSt.revalAfter = pdi.URIDTimestampMax

        shortStr := chSt.ChID().SuffixStr()
        chSt.SetLogLabel(fmt.Sprintf("Ch %s", shortStr))
        chSt.Infof(1, "instantiating channel %v with protocol '%s'", chSt.ChID().Str(), chSt.State.ChProtocol)
    }

    return ch
}





        /*
        chSt = &ChStore{}

        dbTxn := chMgr.db.NewTransaction(true)

        item, err = dbTxn.Get(inChannelID)
        if err == nil {
            err = item.Value(func(val []byte) error {
                return chSt.State.Unmarshal(val)
            })

        // If we didn't find the channel ID, designate (and/or allocate) a ChStore for this channel ID.
        } else if err == badger.ErrKeyNotFound {
            chMgr.State.NumChStores++
            chSt.State.ChStoreID = chMgr.State.NumChStores

            var (
                buf [200]byte
                bufSz int
            )
            bufSz, err = chSt.State.MarshalTo(buf[:])

            // Store the new ChStore ID assignment
		    if err == nil {
                err = dbTxn.Set(inChannelID, buf[:bufSz])
                
                // Store the new ch mgr state
                if err == nil {
                    bufSz, err = chMgr.State.MarshalTo(buf[:])
                    err = dbTxn.Set(chMgrStateKey, buf[:bufSz])
                }
            }
        }

        if err == nil {
            err = dbTxn.Commit()
        } else {
            dbTxn.Discard()
        }*/





// OpenChannelSession instantiates a nre channel session for the given channel ID (and accompanying params)
func (chMgr *ChMgr) OpenChannelSession(
    inMemberSession *MemberSession,
    inInvocation *ChInvocation, 
    inOutlet Repo_OpenChannelSessionServer,
) (*ChSession, error) {

    var (
        err error
        ch ChAgent
    )

    if inInvocation.ChannelGenesis == nil {
        ch, err = chMgr.FetchChannel(inInvocation.ChID)
    } else {
        if ! inInvocation.ChannelGenesis.IsChannelGenesis() {
            err = plan.Error(nil, plan.ParamMissing, "genesis channel epoch has prev epoch ID set")
        }
    }
    if err != nil {
        return nil, err
    }
    

    cs := &ChSession{
        MemberSession: inMemberSession,
        SessionID: atomic.AddUint32(&chMgr.nextSessionID, 1),
        ChAgent: ch,
        msgInbox: make(chan *ChMsg, 1),
        outlet: inOutlet,
        OnComplete: make(chan error),
    }


    err = cs.startup(inInvocation)
    if err != nil {
        return nil, err
    }
    
    if ch == nil {
        if inInvocation.ChannelGenesis == nil {
            err = plan.Error(nil, plan.ParamMissing, "ChSession has no channel agent")
        } else {

            // This will get filled in when the entry is merged after unpacking
            inInvocation.ChannelGenesis.CommunityEpochID = nil

            body, _ := inInvocation.ChannelGenesis.Marshal()
            chMsg := &ChMsg{
                Op: ChMsgOp_CH_NEW_ENTRY,
                SessionID: cs.SessionID,
                ChEntry: &ChEntryMsg{
                    EntryOp: pdi.EntryOp_NEW_CHANNEL_EPOCH,
                    Body: body,
                },
            }

            cs.msgInbox <- chMsg
        }
    }

    cs.SetLogLabel(fmt.Sprint("ChSess-", cs.SessionID))
 
    return cs, err
}



type ChSession struct {
    plan.Logger

    SessionID           uint32

    MemberSession       *MemberSession

    ChAgent             ChAgent

    OnComplete          chan error
    isComplete          bool

    closed              bool

    // The currently open pipes 
    //Pipe                pservice.CommunityRepo_ChSessionPipeServer

    // Close the Inbox to end the channel session.
    // When closure is complete, the sessions msg outbox will be closed.
    msgInbox            chan *ChMsg
 
    //msgOutbox           chan *ChMsg
    outlet              Repo_OpenChannelSessionServer

    log                 *log.Logger
}


func (cs *ChSession) startup(
    inInvocation *ChInvocation,
) error {

    go func() {

        var (
            ctxErr error
        )

        for ! cs.isComplete { 
            select {
                case <- cs.outlet.Context().Done():
                    ctxErr = cs.outlet.Context().Err()
                    cs.CloseSession("ChSession context closed")
                case chMsg := <- cs.msgInbox:
                    if chMsg == nil {
                        if ! cs.isComplete {
                            cs.MemberSession.detachChSession(cs)
                            cs.isComplete = true
                            cs.OnComplete <- ctxErr
                        }
                    } else {
                        switch chMsg.Op {                            
                            case ChMsgOp_CH_NEW_ENTRY:
                                cs.AuthorNewEntry(chMsg)
                        } 
                    }  
            }
        }
    }()

    return nil
} 


func (cs *ChSession) CloseSession(inReason string) {
    if ! cs.closed {
        cs.closed = true
        close(cs.msgInbox)
    }
}


func (cs *ChSession) AuthorNewEntry(
    chMsg *ChMsg,
) error {

    var (
        err error
        chSt *ChStore
        chGenesis bool
    )

    entry := chEntryAlloc(entryWasAuthored)
    
    info := &entry.tmpInfo
    
    entry.Body = append(entry.Body[:0], chMsg.ChEntry.Body...)

    info.SetTimeAuthored(0)
    info.EntryOp = chMsg.ChEntry.EntryOp
    info.EntrySubOp = chMsg.ChEntry.EntrySubOp
    info.SupersedesEntryID = info.SupersedesEntryID[:0]
    info.Extensions = nil

    // Fill in the authoring secion
    copy(info.GetTID(pdi.EntryTID_AuthorEntryID), cs.MemberSession.MemberEpoch.EpochTID)

    if chMsg.ChEntry.EntryOp == pdi.EntryOp_NEW_CHANNEL_EPOCH {
        var chEpoch pdi.ChannelEpoch

        err := chEpoch.Unmarshal(chMsg.ChEntry.Body)
        if err == nil {
            chGenesis = chEpoch.IsChannelGenesis() 
        }
    }


    if cs.ChAgent != nil {
        chSt = cs.ChAgent.Store()
        info.ChannelID = append(info.ChannelID[:0], chSt.ChID()...)

        err = chSt.FetchAuthoringTIDs(
            info.GetTID(pdi.EntryTID_ChannelEpochEntryID),
            info.GetTID(pdi.EntryTID_ACCEntryID),
        )

    } else if chGenesis {
        info.ChannelID = info.ChannelID[:0]

        entry.onMergeComplete = func(entry *chEntry, ch ChAgent, err error) {
            if err == nil {
                cs.ChAgent = ch

                reply := &ChMsg{
                    Op: ChMsgOp_CH_GENESIS_COMPLETE,
                    MsgID: chMsg.MsgID,
                    SessionID: cs.SessionID,
                    BUF0: ch.Store().ChID(),
                    ChEntry: &ChEntryMsg{
                        TIDs: append([]byte{}, entry.Info.TIDs...),
                        EntryOp: entry.Info.EntryOp,
                        EntrySubOp: entry.Info.EntrySubOp,
                        AuthorMemberID: entry.Info.AuthorMemberID,
                        Status: entry.State.Status,
                        Body: append([]byte{}, entry.Body...),
                    },
                }
                cs.outlet.Send(reply)
            } else {
                // TODO
            }
        }
    } else {
        err = plan.Error(nil, plan.AssertFailed, "ch session has no ChAgent")
    }

    if err == nil {
        cs.MemberSession.entriesOutbound <- entry
    }

    return err
}






type EntryRange struct {
    Start   pdi.URID        // Inclusive
    end     pdi.URID        // Inclusive
}

/*
channels:
- contain a list of all entries posted to them.  this list is ordered by URID  
- each entry is not-yet-analyzed, live, deferred, auto-rejected, or manually-rejected, .  callbacks for when entry status changes (sucks but req'd)
- entries that are deferred and rejected maintain stats for strategic retries, etc
     - repo-wide reject db?  YES, this would allow smart scheduling re attempts, logging, and warning, etc.
     - OR, all entries live in the channel db, but records of deferrals, rejects, and to-be-processed entries are in a scrap db that manages and initiates retries. 
         - Con: two dbs could get out of sync
- => entries that are live, live in the channel db.  entries that are yet-to-be analyzed wait in 
- each entry: channel ID (Allowing multple channels in the same DB), entry URID, EntryOp, entry status, EntryInfo (small buf) and Body (var buf) -- EntryInfo is needed for validation
- entry key name:  chID + entry URID 
- entry status:  posted, removed, replaced (with entry URID)

*/



