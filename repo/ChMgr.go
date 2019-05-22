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

    //close(chMgr.channelsToRevalidate)

}

type chEntryFlags int32
const (

	writeEntry  chEntryFlags = 1 << iota
    writeEntryState  
)

type chEntry struct {
    Body                []byte
    
    Info                ChEntryInfo
    State               ChEntryState
    StatePrev           ChEntryState
    
    //Asset               pcore.Marshaller
    newChEpoch          *pdi.ChannelEpoch
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
    entry.Info.TIDs                 = inEntryInfo.TIDs
    entry.Info.AuthorMemberID       = inEntryInfo.AuthorMemberID
    entry.Info.SupersedesEntryID    = inEntryInfo.SupersedesEntryID
    entry.Info.AuthorSig            = inAuthorSig
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

    entry.newChEpoch = nil
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

// RequiresGenesisAuthority retruns true if this entry does not specify the required TIDs normally needed to authorize and validate a channel entry.
func (entryInfo *ChEntryInfo) RequiresGenesisAuthority() bool {
    return len(entryInfo.TIDs) < int(pdi.EntryTID_NormalNumTIDs) * plan.TIDSz
}

// GetTID returns a slice to requested EntryTID
func (entryInfo *ChEntryInfo) GetTID(inID pdi.EntryTID) plan.TID {
    return entryInfo.TIDs[inID*plan.TIDSz:]
}

// EntryID returns this entry's TID.
func (entryInfo *ChEntryInfo) EntryID() plan.TID {
    return entryInfo.GetTID(pdi.EntryTID_EntryID)
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
        chEpoch *pdi.ChannelEpoch
        err error
        //isChGenesis bool
    )

    switch entry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH: {
            chEpoch = &pdi.ChannelEpoch{}
            if err = chEpoch.Unmarshal(entry.Body); err != nil {
                err = plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling NEW_CHANNEL_EPOCH")
                entry.ThrowMalformed(err)
            } else {
                chEpoch.EpochTID = entry.Info.EntryID()
                entry.newChEpoch = chEpoch
    
                // Is the the channel genesis epoch?  If so, the channel ID derives from the channel ID.
                if chEpoch.IsChannelGenesis() {
                    chID = chEpoch.EpochTID[plan.TIDSz - plan.ChIDSz:]
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
        ch, err = chMgr.FetchChannel(chID, chEpoch)

        // If it's a channel that is yet to know its protocol but now have that, "reboot" the channel with the proper agent.
        if chEpoch != nil {
            chUnknown, _ := ch.(*ChUnknown)
            if chUnknown != nil {
                ch, err = chMgr.rebootChannel(chID, chEpoch)
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

    ch, err := chMgr.FetchChannel(inChID, nil)
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



func (chMgr *ChMgr) FetchMemberRegistry() (*ChMemberRegistry, error) {

    //var epoch *pdi.MemberEpoch

    ch, err := chMgr.FetchChannel(chMgr.StorageEpoch.MemberRegistry(), nil)
    if err == nil {
        return nil, err
    }

    memberRegistry := ch.(*ChMemberRegistry)

    return memberRegistry, nil
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
}



// FetchChannel returns the owning ChAgent for the given channel ID.
// If inGenesisEpoch is set, channel genesis is performed. 
func (chMgr *ChMgr) FetchChannel(
    inChID plan.ChID,
    inChEpoch *pdi.ChannelEpoch,
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

    chMgr.chMutex.Lock()
    defer chMgr.chMutex.Unlock()
    
    // Recheck to handle race condition
    ch = chMgr.chLoaded[chID]

    // TODO: channel genesis dupes?

    // If the ch store isn't already loaded, try to load it.
    // To do that, we need to need to lookup it's ChStoreID
    if ch == nil { 
        ch, err = chMgr.loadChannel(inChID, inChEpoch)

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



func (chMgr *ChMgr) loadChannel(
    inChID plan.ChID,
    inChEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    /*
    chSt := &ChStore{
        chMgr: chMgr,
        revalQueue: make(chan revalQueue, 10),
    }*/

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
            chState.ChannelID = append(chState.ChannelID[:0], inChID...)
    
            if inChEpoch != nil {
                chState.ChProtocol = inChEpoch.ChProtocol
            }
        }

        chTxn.Discard()
    }
    

    if err == nil {

        agentFactory := gChAgentRegistry[chState.ChProtocol]
        if agentFactory != nil {
            ch = agentFactory(chState.ChProtocol)
        } 

        mergeEnabled := true
        if ch == nil {
            if inChEpoch != nil {
                panic("TODO: implement channel protocol not known")
            }

            ch = &ChUnknown{}
            mergeEnabled = false
        }

        {
            chSt := ch.Store()
            chSt.State = chState
            chSt.db = chDb
            chSt.chMgr = chMgr
            chSt.entriesToMerge = make(chan *chEntry, 2)
            chSt.epochCache = map[plan.TIDBlob]*pdi.ChannelEpoch{}

            // If we're loading the channel and discover that merge has never been enabled, reset the validation bookmark.
            if ! chSt.State.MergeEnabled && mergeEnabled {
                chSt.State.ValidatedUpto = nil
            }

            chSt.State.MergeEnabled = mergeEnabled
            chSt.revalAfter = pdi.URIDTimestampMax

            err = ch.Startup()

            go MergeAndValidate(ch)
        }
    }

    if err != nil {

       // TODO 
        /*
        if chSt.db != nil {
            chSt.db.Close()
            chSt.db = nil
        } */
        ch = nil
    }

    return ch, err
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

    ch, err := chMgr.FetchChannel(inInvocation.ChID, nil)
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



