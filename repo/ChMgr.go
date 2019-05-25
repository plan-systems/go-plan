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
    //"fmt"
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



/*
// revalMsg tells a channel what entries need revalidation.
//
// This can be more sophisticated in the future for explicit entries
type revalMsg struct {
    revalMode               revalMode
    revalAfter              int64
}
*/


// ChMgr is the top level interface for a community's channels.
type ChMgr struct {
    flow                    plan.Flow

    HomePath                string

    nextSessionID           uint32
    StorageEpoch            pdi.StorageEpoch               


    //db                      *badger.DB
    State                   ChMgrState

    //fsNameEncoding          *base64.Encoding


   // revalQueue              chan revalidateMsg
    chMutex                 sync.RWMutex
    chLoaded                map[plan.ChIDBlob]ChAgent

}





var (
    //hMgrStateKey   = []byte{0}
    zero64          = []byte{0, 0, 0, 0, 0, 0, 0, 0}
)

func NewChMgr(
    inHomeDir string,
) *ChMgr {

    chMgr := &ChMgr{
        chLoaded: make(map[plan.ChIDBlob]ChAgent),
        HomePath: path.Join(inHomeDir, "ChMgr"),
        nextSessionID: 0,
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


/*
    //
    //
    //
    //
    // channel (re)validator
    //
    chMgr.channelsToRevalidate = make(chan plan.ChannelID, 20)
    chMgr.flow.ShutdownComplete.Add(1)
    go func() {

        for chID := range chMgr.channelsToRevalidate {
            ch, err := chMgr.FetchChannel(chID, nil)
            if err != nil {
                chMgr.flow.LogErr(err, "error fetching channel for revalidation")
                continue
            }

            chMgr.Revalidate()
        }

        chMgr.flow.ShutdownComplete.Done()
    }()*/


    return nil
}
  

func (chMgr *ChMgr) onInternalShutdown() {


    // The assumption at this point is that no new entries are being sent to channels to be merged.
    // This means that we can close each channel's entriesToMerge to proceed with shutdown
    var waitingOn []*ChStore
    for {

        shutdown := &sync.WaitGroup{} 

        // First, cause all the ChStores to exit their main validation loop.
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

        // Wait until every loaded channel has exited it main validation loop
        shutdown.Wait()

        // At this point, nothing is running (so no futher calls to FetchChannel() should/will be made), so now we can fully shutdown each channel.
        shutdown.Add(len(waitingOn))
        for _, chSt := range waitingOn {
            chMgr.detachChannel(chSt.ChannelID())
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
type chEntryFlags int32
const (

	writeEntry  chEntryFlags = 1 << iota
    writeEntryState  
)
*/
type chEntry struct {
    Body                []byte
    
    Info                ChEntryInfo
    State               ChEntryState
    StatePrev           ChEntryState
    
    scrap               []byte
    
    //Asset               pcore.Marshaller
    //newChEpoch          *pdi.ChannelEpoch
    flushState          bool
    flushBody           bool
    //txn                 *badger.Txn
    //scrap               []byte

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
    entry.Info.TIDs                 = append(entry.Info.TIDs[:0], inEntryInfo.TIDs...)
    entry.Info.AuthorMemberID       = inEntryInfo.AuthorMemberID
    entry.Info.SupersedesEntryID    = append(entry.Info.SupersedesEntryID[:0], inEntryInfo.SupersedesEntryID...)
    entry.Info.AuthorSig            = append(entry.Info.AuthorSig[:0], inAuthorSig...)
    entry.Info.Extensions           = inEntryInfo.Extensions

    entry.State.Status              = ChEntryStatus_AWAITING_MERGE
    entry.State.Flags               = 1 << byte(ChEntryFlag_WELL_FORMED)
    entry.State.LiveIDs             = entry.State.LiveIDs[:0]

    entry.StatePrev                 = entry.State
    entry.flushState                = true
    entry.flushBody                 = true

}


func (entry *chEntry) Reuse() {

    entry.Info.SupersedesEntryID = entry.Info.SupersedesEntryID[:0]
    entry.Info.Extensions = nil
    entry.Body = entry.Body[:0]
    entry.State.LiveIDs = entry.State.LiveIDs[:0]

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
        entry.Touch()
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
        entry.Touch()
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
        entry.Touch()
    }
}

// Defer marks this entry as deferred for the given reason/error.
func (entry *chEntry) Defer(inErr error) {
    entry.SetStatus(ChEntryStatus_DEFERRED)
}

// Touch increments .Rev to reflect that this entry has been modified
func (entry *chEntry) Touch() {
    entry.flushState = true
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




/*
// ChEntryFlagsLiveFlags reppresent the conditions needed for a channel entry to be considered live.
const ChEntryFlagsLiveFlags = 
    (1 << byte(ChEntryFlag_AUTHOR_VALIDATED)) |
    (1 << byte(ChEntryFlag_CHANNEL_EPOCH_VALIDATED)) | 
    (1 << byte(ChEntryFlag_ACC_VALIDATED)) | 
    (1 << byte(ChEntryFlag_AGENT_VALIDATED)) |
    (1 << byte(ChEntryFlag_WELL_FORMED))
*/



// QueueEntryForMerge is called when an entry arrives to a repo and must be merged into the repo.
func (chMgr *ChMgr) QueueEntryForMerge(
    chID plan.ChID,
    entry *chEntry,
) {

    var (
        ch ChAgent
        chGenesisEpoch *pdi.ChannelEpoch
        err error
        //isChGenesis bool
    )

    switch entry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH: {
            chEpoch := &pdi.ChannelEpoch{}
            if err = chEpoch.Unmarshal(entry.Body); err != nil {
                err = plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling NEW_CHANNEL_EPOCH")
                entry.ThrowMalformed(err)
            } else {
                chEpoch.EpochTID = entry.Info.EntryID()
                //entry.newChEpoch = chEpoch
    
                // Is the the channel genesis epoch?  If so, the channel ID derives from the channel ID.
                if chEpoch.IsChannelGenesis() {
                    chID = chEpoch.EpochTID[plan.TIDSz - plan.ChIDSz:]
                    chGenesisEpoch = chEpoch
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
                    chUnk.Infof("channel protocol %v not recognized", chGenesisEpoch.ChProtocol)
                } else {

                    // Stop entry processing for the ChUnknown we want to take off service
                    chUnk.Infof("Rebooting channel")
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
) (*ChACC, error) {

    var acc *ChACC

    ch, err := chMgr.FetchChannel(inChID)
    if ch != nil {
        acc, _ := ch.(*ChACC)
        if acc == nil {
            err = plan.Errorf(nil, plan.NotAnACC, "cited channel %v is not an access control channel", inChID)
        }
    }

    if err != nil {
        return nil, err
    }

    return acc, nil
}


// FetchMemberRegistry returns the community's member registry channel
func (chMgr *ChMgr) FetchMemberRegistry() (*ChMemberRegistry, error) {

    ch, err := chMgr.FetchChannel(chMgr.StorageEpoch.MemberRegistry())
    if err == nil {
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


    if ch != nil || ! inAutoCreate {
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
        ch, err = chMgr.loadChannel(inChID, inChGenesis)

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
    inChGenesis *pdi.ChannelEpoch,
) (ChAgent, error) {

    opts := badger.DefaultOptions
    opts.Dir = path.Join(
        chMgr.HomePath,
        inChID.String(),
    )
    opts.ValueDir = opts.Dir

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
        chSt.epochCache = map[plan.TIDBlob]*pdi.ChannelEpoch{}

        // If we're loading the channel and discover that merge has never been enabled, reset the validation bookmark.
        if ! chSt.State.MergeEnabled && mergeEnabled {
            chSt.State.ValidatedUpto = nil
        }
        
        chSt.State.MergeEnabled = mergeEnabled
        chSt.State.ChProtocol = inChState.ChProtocol
        chSt.State.ChannelID = append(chSt.State.ChannelID[:0], inChState.ChannelID...)
        chSt.revalAfter = pdi.URIDTimestampMax
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
    inInvocation *ChInvocation, 
    inOutlet Repo_OpenChannelSessionServer,
) (*ChSession, error) {

    ch, err := chMgr.FetchChannel(inInvocation.ChID)
    if err != nil {
        return nil, err
    }

    chSession := &ChSession{
        SessionID: atomic.AddUint32(&chMgr.nextSessionID, 1),
        ChAgent: ch,
        msgInbox: make(chan *ChMsg, 1),
        outlet: inOutlet,
    }

    err = chSession.Startup(inInvocation)
    if err != nil {
        return nil, err
    }

    return chSession, err
}





type ChSession struct {

    SessionID           uint32

    MemeberSession      *MemberSession

    ChAgent             ChAgent

    //ChStore             *ChStore

    //ChAdapter           ChAdapter

    // The currently open pipes 
    //Pipe                pservice.CommunityRepo_ChSessionPipeServer

    // Close the Inbox to end the channel session.
    // When closure is complete, the sessions msg outbox will be closed.
    msgInbox            chan *ChMsg
 
    //msgOutbox           chan *ChMsg
    outlet              Repo_OpenChannelSessionServer

    log                 *log.Logger
}


func (cs *ChSession) Startup(
    inInvocation *ChInvocation,
) error {

    go func()
    //cs.ChAgent.
    return nil
} 



/*

\


func (ch *ChCustodian) Startup() error {

    return ch.internalStatup()

}
func (ch *ChCustodian) internalStatup() error {


    {
        opts := badger.DefaultOptions
        opts.Dir = path.Join(ch.HomePath, "chDB")
        opts.ValueDir = opts.Dir

        var err error
        if ch.chDB, err = badger.Open(opts); err != nil {
            return plan.Error(err, plan.StorageNotReady, "channel db failure")
        }
    }
    //
    //
    //
    //
    go func() {
        for eip := range ch.entryInbox {
            if ! bytes.Equal(ch.ChannelID[:], eip.EntryInfo.ChannelId) {
                panic("channel entry sent to wrong channel")
            }
            dbTxn := ch.chDB.NewTransaction(true)
            dbErr := dbTxn.Set([]byte(eip.EntryInfo.URID), nil)//txn.Bytes)
            if dbErr == nil {
                dbErr = dbTxn.Commit()
            } else {
                dbTxn.Discard()
            }

        }


    }()


    return nil
}





func (ch *ChCustodian) PostEntry(
    inInfo *pdi.EntryInfo,
    inBody []byte,
) error {


    return nil
}

*/




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



