package repo


import (
    //"bytes"
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



type chStoreFlags int32
const (
    chReadOnlyAccess        chStoreFlags = 0x01
    chWriteAccess           chStoreFlags = 0x02
    chCitedAsAccessChannel  chStoreFlags = 0x10
)


// revalMsg tells a channel what entries need revalidation.
//
// This can be more sophisticated in the future for explicit entries
type revalMsg struct {
    chID                    plan.ChannelID
    revalAfter              int64
}



// ChMgr is the top level interface for a community's channels.
type ChMgr struct {
    flow                    plan.Flow

    HomePath                string

    nextSessionID           uint32                  


    //db                      *badger.DB
    State                   ChMgrState

    //fsNameEncoding          *base64.Encoding


    revalQueue              chan revalidateMsg
    lookupMutex             sync.RWMutex
    hotChannels             map[plan.ChannelID]ChAgent

}





var (
    //hMgrStateKey   = []byte{0}
    zero64          = []byte{0, 0, 0, 0, 0, 0, 0, 0}
)

func NewChMgr(
    inHomeDir string,
) *ChMgr {

    chMgr := &ChMgr{
        hotChannels: make(map[plan.ChannelID]ChAgent),
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


type revalidateMsg struct {
    chID        plan.ChannelID
    after       int64
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
    }()


    return nil
}
  

func (chMgr *ChMgr) onInternalShutdown() {

    close(chMgr.channelsToRevalidate)

}



func (chEntry *ChEntryInfo) AssignFromDecrytedEntry(
    inEntryInfo *pdi.EntryInfo,
    inAuthorSig []byte,
) {

    chEntry.TimeAuthored        = inEntryInfo.TimeAuthored
    chEntry.TimeAuthoredFrac    = inEntryInfo.TimeAuthoredFrac
    chEntry.EntryOp             = inEntryInfo.EntryOp
    chEntry.ChannelEpochID      = inEntryInfo.ChannelEpochID
    chEntry.ACCEpochID          = inEntryInfo.ACCEpochID
    chEntry.AuthorMemberID      = inEntryInfo.AuthorMemberID
    chEntry.AuthorMemberEpoch   = inEntryInfo.AuthorMemberEpoch
    chEntry.BodyEncoding        = inEntryInfo.BodyEncoding
    chEntry.AuthorSig           = inAuthorSig
    chEntry.Status              = ChEntryStatus_AWAITING_MERGE
    chEntry.Flags               = 1 << byte(ChEntryFlag_WELL_FORMED)

}


// MissingFlag returns true if the given flag is NOT set
func (chEntry *ChEntryInfo) MissingFlag(inFlag ChEntryFlag) bool {
    flag := uint32(1) << byte(inFlag)
    return (chEntry.Flags & flag) == 0
}

// ClearFlag clears the given flag
func (chEntry *ChEntryInfo) ClearFlag(inFlag ChEntryFlag) {
    flag := uint32(1) << byte(inFlag)
    chEntry.Flags &^= flag
}

// HasFlag returns true if the given flag is set
func (chEntry *ChEntryInfo) HasFlag(inFlag ChEntryFlag) bool {
    flag := uint32(1) << byte(inFlag)
    return (chEntry.Flags & flag) == flag
}

// HasFlags returns true if ALL of the passed flags are present
func (chEntry *ChEntryInfo) HasFlags(inFlags...ChEntryFlag) bool {

    chFlags := chEntry.Flags

    for _, flag := range inFlags {
        bit := uint32(1) << uint8(flag)
        if (chFlags & bit) != bit {
            return false
        }
    }

    return true
}

// AddFlags ORs the given flags to this
func (chEntry *ChEntryInfo) AddFlags(inFlags...ChEntryFlag) {

    addFlags := uint32(0)
    for _, flag := range inFlags {
        addFlags |= uint32(1) << uint8(flag)
    }

    chEntry.Flags |= addFlags
}

// ThrowMalformed marks this entry as disbarred due to not being self-consistent for the given reason.
func (chEntry *ChEntryInfo) ThrowMalformed(inErr error) {
    chEntry.ClearFlag(ChEntryFlag_WELL_FORMED)
    chEntry.Status = ChEntryStatus_DISBARRED
}

// Defer marks this entry as deferred for the given reason/error.
func (chEntry *ChEntryInfo) Defer(inErr error) {
    chEntry.Status = ChEntryStatus_DEFERRED
}

// IsLive returns true if the requisite flags are present for a channel entry to be live.
func (chEntry *ChEntryInfo) IsLive() bool {
    return (chEntry.Flags & ChEntryFlagsLiveFlags) == ChEntryFlagsLiveFlags
}

// IsWellFormed returns true if ChEntryFlag_WELL_FORMED is set for this entry.
func (chEntry *ChEntryInfo) IsWellFormed() bool {
    return (chEntry.Flags & (1 << byte(ChEntryFlag_WELL_FORMED))) != 0
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




// MergeEntry is called when an entry arrives to a repo and must be merged into the repo.
func (chMgr *ChMgr) MergeEntry(
    ioEntry *DecryptedEntry,
) {

    var (
        ch ChAgent
        entryErr, processErr error
        chGenesis *pdi.ChannelEpoch
    )



    switch ioEntry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH: {
            chGenesis = &pdi.ChannelEpoch{}
            if entryErr := chGenesis.Unmarshal(ioEntry.Body); entryErr != nil {
                entryErr = plan.Error(entryErr, plan.ChEntryIsMalformed, "error unmarshalling NEW_CHANNEL_EPOCH")
                ioEntry.ChEntry.ThrowMalformed(entryErr)
            } else {
                ioEntry.ChEntry.DepKey = chGenesis.EpochID
            }
        }
    }

    if ioEntry.ChEntry.IsWellFormed() {
        ch, entryErr = chMgr.FetchChannel(ioEntry.ChannelID(), chGenesis)
        if entryErr != nil {
            ioEntry.ChEntry.Defer(entryErr)
        } else {
            entryErr, processErr = chMgr.ValidateEntry(ch, &ioEntry.ChEntry, true) //ioEntry.URID
        }
    }

    // TODO: IF deferred or malformed already, stop processing 
    if ch == nil {

    } else {
        chTxn := ch.Store().StartMerge()

        switch ioEntry.Info.EntryOp {
            
            case pdi.EntryOp_POST_CONTENT:
                entryErr, processErr = ch.PostEntry(chTxn, ioEntry)
                if entryErr != nil {
                    ioEntry.ChEntry.ThrowMalformed(entryErr)
                }
            
        }

        if entryErr == nil && processErr == nil {
            ioEntry.ChEntry.AddFlags(ChEntryFlag_AGENT_VALIDATED)        
        }

        if processErr == nil {
            processErr = ch.Store().WriteEntryRaw(chTxn, ioEntry)
        }

        if processErr == nil {
            processErr = chTxn.Commit()

            
            if ioEntry.ChEntry.Status == ChEntryStatus_LIVE {

                if ioEntry.ChannelID() == plan.MemberRegistryChannelID {

                } else {

                    // Are there channels awaiting this entry?
                    ch.RevalidateDependencies(entryChID, &ioEntry.ChEntry)

                }
        } else {
            chTxn.Discard()
        }
    }



    chMgr.flow.LogErr(entryErr, "error merging entry")
    chMgr.flow.LogErr(processErr, "error processing entry")
}






func (chMgr *ChMgr) ValidateEntry(
    ch ChAgent,
    ioEntry *ChEntryInfo,
    inWriteDependencies bool,
) (entryErr error, processErr error) {


    var (
        accID uint64
        authURID []byte
        entryChID plan.ChannelID
        //err error
        //authorEpoch *pdi.MemberEpoch
    )

    status := ChEntryStatus_LIVE

    if ! inChEntry.ChEntry.HasFlags(ChEntryFlag_IS_GENESIS_ENTRY, ChEntryFlag_WELL_FORMED) {
        
        if inWriteDependencies {
            entryChID = ch.Store().State.ChannelID
        }

        // 1 -- Check community membership records
        {
            registry, err := chMgr.FetchMemberRegistry()
            if err != nil {
                processErr = err
            } else {
                err = registry.ValidateAuthor(inChEntry)

                // TODO: handle when member registry returns an werr
                if inWriteDependencies {
                    processErr = registry.AddDependency(ChDependencyType_BY_AUTHOR, entryChID, ioEntry)
                }
            }
            if err != nil {
                status = ChEntryStatus_DEFERRED
            }
        }

        // 2 -- Check entry's parent channel governing epoch
        {
            chEpoch, err := ch.CanAcceptEntry(*ioEntry)
            if err != nil {
                processErr = err

                status = ChEntryStatus_DEFERRED

            } else {

                // Note: no dependency is created here b/c if/when a channel entry is overturned, all subsequent entries in that channel undergo revalidation,
                accID = chEpoch.AccessChannelID
            }
                            
            if inWriteDependencies {
                processErr = ch.AddDependency(ChDependencyType_BY_CHANNEL, entryChID, ioEntry)
            }
        }

        // 3 -- Check parent ACC
        {
            acc, err := chMgr.FetchACC(accID)
            if err == nil {
                err = acc.IsEntryAuthorized(inChEntry)
                if inWriteDependencies {
                    processErr = acc.AddDependency(ChDependencyType_BY_ACC, entryChID, ioEntry)
                }
            } 
            
            if err != nil {
                status = ChEntryStatus_DEFERRED
            }
        }
    }

    if ioEntry.Status != status {
        ioEntry.Status = status
    }

    return processErr
}


func (chMgr *ChMgr) revalidateChannel(
    chID plan.ChannelID,
) { 



}


func (chMgr *ChMgr) FetchACC(
    inChannelID uint64,
) (*ChACC, error) {

    var acc *ChACC

    ch, err := chMgr.FetchChannel(plan.ChannelID(inChannelID), nil)
    if ch != nil {
        acc, _ := ch.(*ChACC)
        if acc == nil {
            err = plan.Errorf(nil, plan.NotAnACC, "cited channel %v is not an access control channel", inChannelID)
        }
    }

    if err != nil {
        return nil, err
    }

    return acc, nil
}



func (chMgr *ChMgr) FetchMemberRegistry() (*ChMemberRegistry, error) {

    //var epoch *pdi.MemberEpoch

    ch, err := chMgr.FetchChannel(plan.MemberRegistryChannelID, nil)
    if err == nil {
        return nil, err
    }

    memberRegistry := ch.(*ChMemberRegistry)

    return memberRegistry, nil
}


// FetchChannel returns the owning ChAgent for the given channel ID.
// If inGenesisEpoch is set, channel genesis is performed. 
func (chMgr *ChMgr) FetchChannel(
    inChannelID plan.ChannelID,
    inGenesisEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    var (
        err error
    )

    chMgr.lookupMutex.RLock()
    ch := chMgr.hotChannels[inChannelID]
    chMgr.lookupMutex.RUnlock()

    if ch != nil {
        return ch, nil
    }

    chMgr.lookupMutex.Lock()
    defer chMgr.lookupMutex.Unlock()
    
    // Recheck to handle race condition
    ch = chMgr.hotChannels[inChannelID]

    // TODO: channel genesis dupes?

    // If the ch store isn't already loaded, try to load it.
    // To do that, we need to need to lookup it's ChStoreID
    if ch == nil { 
        ch, err = chMgr.loadChannel(inChannelID, inGenesisEpoch)

        // We've already mutexed for this write condition (above)
        if err == nil {
            chMgr.hotChannels[inChannelID] = ch
        }  
    }
    
    // TODO: rebuild channel if db load fails?
    if err != nil {
        ch = nil
    }

    return ch, err
}


func (chMgr *ChMgr) loadChannel(
    inChannelID plan.ChannelID,
    inGenesisEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    chSt := &ChStore{
    }

    memberID, issuedID := inChannelID.Unplex() 

    opts := badger.DefaultOptions
    opts.Dir = path.Join(
        chMgr.HomePath,
        fmt.Sprintf("%04d/%05d", memberID, issuedID),
    )
    opts.ValueDir = opts.Dir

    if err := os.MkdirAll(opts.Dir, plan.DefaultFileMode); err != nil {
        return nil, plan.Error(err, plan.FailedToLoadChannel, "failed to create channel dir")
    }

    chDb, err := badger.Open(opts)
    if err != nil {
        err = plan.Error(err, plan.FailedToLoadChannel, "failed to open channel db")
    }

    var ch ChAgent

    if err == nil {
        ch, err = InstantiateChAgent(inChannelID, chDb, inGenesisEpoch)
    }

    if err != nil {
        if chSt.db != nil {
            chSt.db.Close()
            chSt.db = nil
        }
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

    ch, err := chMgr.FetchChannel(plan.ChannelID(inInvocation.ChID), nil)
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



