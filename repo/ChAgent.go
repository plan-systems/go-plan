package repo

import (
    "bytes"
    //"log"
    //"os"
    //"path"
    //"io/ioutil"
    //"strings"
    "sync"
    //"sync/atomic"
    //"context"
    "fmt"
    "sort"
   //"reflect"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"
    //"encoding/binary"
    

    "github.com/plan-systems/go-ptools"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    //"github.com/plan-systems/go-plan/pservice"
    //"github.com/plan-systems/go-plan/ski"


    "github.com/dgraph-io/badger"

)



// ChAgentFactory is a ChAgent factory function. 
// It returns a new ChAgent instance associated with the given channel protocol invocation.
type ChAgentFactory func(inChProtocol string) ChAgent

// ChAgentRegistry is a read-only registry that maps a channel protocol string to a ChAgentFactory.
// This is used when a ChStore is being created and requires an accompanying ChAgent.
var gChAgentRegistry = map[string]ChAgentFactory{}



const (

    // BuiltinProtocolPrefix is the prefix used for all builtin channel protocols.
    BuiltinProtocolPrefix           = "ch/_/"

    // ChProtocolACC is used for RootACChannelID and all other ACCs
    ChProtocolACC                   = BuiltinProtocolPrefix + "acc"

    // ChProtocolMemberRegistry is used for plan.MemberRegistryChannelID
    ChProtocolMemberRegistry        = BuiltinProtocolPrefix + "member-registry"

    // ChProtocolCommunityEpochs is used for plan.MemberRegistryChannelID
    ChProtocolCommunityEpochs       = BuiltinProtocolPrefix + "community-epochs"

    // ChProtocolTalk is for conventional group chat sequential text streams
    ChProtocolTalk                  = BuiltinProtocolPrefix + "talk"

    // ChProtocolNotes is a set of text-based "pages" that (similar to iOS's notes) 
    ChProtocolNotes                 = BuiltinProtocolPrefix + "notes"

    // ChProtocolSpace is a navigable spatial container/vessel such that each entry is pinned/linked to areas, parameters, or points,
    ChProtocolSpace                 = BuiltinProtocolPrefix + "space"


)


// ChAgent is a protocol-specific implementation built to interact with a ChStore
type ChAgent interface {

    //AttachToChStore(chSt *ChStore)
    Store() *ChStore

    Startup() error

    //NewAssetForContent() pcore.Marshaller

    OnLivenessChanged(
        entry *chEntry,
    ) error


    MergePost(
        entry *chEntry,
    ) error


    MergeNewChannelEpoch(
        entry *chEntry,
    ) error

}


const (

    chEntryInfoKey byte = iota
    chEntryStateKey
    chEntryBodyKey
    chEntryDepsKey

    chEpochKeySz        = 4 + plan.TIDSz    // len(chEpochsPrefix) + len(epochTID)
    chEntryKeySz        = plan.TIDSz + 1
    chEntryTypePos      = plan.TIDSz
    chEntryDepKeySz     = chEntryKeySz + plan.ChIDSz
)


var (
    chStatePrefix   = []byte{0, 0, 0,}
    chBootstrapKey  = append(chStatePrefix, 0x01)
    chStateKey      = append(chStatePrefix, 0x02)
    chEpochsPrefix  = append(chStatePrefix, 0xFF)

    chEntryStart    = []byte{0, 0, 1, 0, 0, 0, 0, 0}
)


/*
ChStore.db 
    |
    + chBootstrapKey                            => ChStoreBootstrap
    |
    + chStateKey                                => ChStoreState
    |
    + chEpochsPrefix + epoch TID 1
    |                + epoch TID 2
    |                ...
    | 
    + chAgentAssetKey + chAgentAssetID A        => custom...
    |                 + chAgentAssetID B        => custom...
    |                 ...
    |
    + chEntryStart
    |
    + EntryTID 1 + chEntryInfoKey               => entry.Info (EntryInfo)
    |            |
    |            + chEntryStateKey              => EntryState    
    |            |
    |            + chEntryBodyKey               => entry.Body
    |            |
    |            + chEntryDepsKey + dep chID 1  => MostLimiting(ChDependency)
    |                             + dep chID 2  => MostLimiting(ChDependency)
    |                             ...
    |
    |
    |+ EntryTID 2 + ...
    |             |
    |             ...
    |
    ...


*/


// ChStore is the low-level channel entry manager for a channel.  
type ChStore struct {
    ptools.Logger

    State                   ChStoreState
    
    // A Ch store needs to have each active channel sessions available so changes/notifications can be sent out.
    chSessions              []*ChSession
    chSessionsMutex         sync.RWMutex 

    // ChEntry keyed by entry URID
    db                     *badger.DB

    // ChannelEpochs sorted by epoch TID 
    chEpochNodes            []*ChEpochNode 
    chEpochMutex            sync.RWMutex

    //epochCache              map[plan.TIDBlob]*pdi.ChannelEpoch 
    //revalGuide              revalGuide  

    chMgr                   *ChMgr

    revalRequestMutex       sync.Mutex 
    revalAfter              plan.TimeFS


    //revalQueue              chan revalMsg
    //readyToMerge            bool
    entriesToMerge          chan *chEntry
    processingEntries       bool

    //chCmdQueue              chan chCmd

    chShutdown              sync.WaitGroup
    //agent                   ChAgent
    //HomePath                 string


    //entryInbox              chan *entryIP


    //ChannelID               plan.ChannelID

}


func (chSt *ChStore) attachChSession(inChSess *ChSession) {
    
    chSt.chSessionsMutex.Lock()
    chSt.chSessions = append(chSt.chSessions, inChSess)
    chSt.chSessionsMutex.Unlock()
}


func (chSt *ChStore) detachChSession(inChSess *ChSession) {
    
    chSt.chSessionsMutex.Lock()
    found := -1
    for i, cs := range chSt.chSessions {
        if cs == inChSess {
            found = i
            break
        }
    }
    if found >= 0 {
        N := len(chSt.chSessions)
        copy(chSt.chSessions[found:], chSt.chSessions[found+1:N])
        N--
        chSt.chSessions[N] = nil
        chSt.chSessions = chSt.chSessions[:N]
    }
    chSt.chSessionsMutex.Unlock()
}


// Log is a convenience function to log errors.  
// This function has no effect if inErr == nil.
func (chSt *ChStore) Log(inErr error, inMsg string) {
    if inErr != nil {
        chSt.Error(inMsg, ": ", inErr)
    }
}

// Store -- see ChAgent.Startup()
func (chSt *ChStore) Store() *ChStore {
    return chSt
}

// ChID returns the channel ID for this ChStore
func (chSt *ChStore) ChID() plan.ChID {
    return plan.ChID(chSt.State.ChannelID)
}

// Startup -- see ChAgent.Startup()
func (chSt *ChStore) Startup() error {
    return nil
}

func (chSt *ChStore) ShutdownEntryProcessing(inBlockUntilComplete bool) {

    // Cause the ch validator loop to wake and get nil msgs
    if chSt.processingEntries {
        chSt.processingEntries = false
        close(chSt.entriesToMerge)
    }

    // TODO: we could just use an atomic state int instead and int32 poll it (since channel shutdown is rare)
    if inBlockUntilComplete {
        chSt.chShutdown.Wait()
    }
}

// Shutdown initiates a channel shutdown and blocks until complete.
// If inWait is given, then this function will call inWait.Done() before exiting
func (chSt *ChStore) Shutdown(inWait *sync.WaitGroup) {
    
    chSt.ShutdownEntryProcessing(true)

    // Update ChState before we go
    chSt.dequeueRevalidation()

    if chSt.db != nil {
        buf, _ := chSt.State.Marshal()

        {
            chTxn := chSt.NewWrite()

            err := chTxn.Set(chStateKey, buf)
            chSt.Log(err, "writing ChState")

            err = chTxn.Commit()
            chSt.Log(err, "committing ChState")
        }

        chSt.db.Close()
        chSt.db = nil
    }

    if inWait != nil {
        inWait.Done()
    }
}


// NewWrite -- see ChAgent.NewWrite
func (chSt *ChStore) NewWrite(
) *badger.Txn {

    chTxn := chSt.db.NewTransaction(true)

    return chTxn
}


// LoadEntry reads the given entry ID from this ChStore.
func (chSt *ChStore) loadEntry(
    inEntryID plan.TID,
    inTxn    *badger.Txn,
    outInfo  *pdi.EntryInfo, 
    outState *EntryState,
    loadBody func(v []byte) error,
) error {

    var (
        entryKey [chEntryKeySz]byte
        item *badger.Item
        err error
    )

    copy(entryKey[:plan.TIDSz], inEntryID)

    chTxn := inTxn
    if chTxn == nil {
        chTxn = chSt.db.NewTransaction(false)
        defer chTxn.Discard()
    }

    // EntryInfo
    if outInfo != nil && err == nil {

        // Reset all fields
        outInfo.EntryOp = pdi.EntryOp(0)
        outInfo.EntrySubOp = 0
        outInfo.SupersedesEntryID = outInfo.SupersedesEntryID[:0]
        outInfo.Extensions = nil
        outInfo.AuthorSig = outInfo.AuthorSig[:0]
        
        entryKey[chEntryTypePos] = chEntryInfoKey
        item, err = chTxn.Get(entryKey[:])
        if err == nil {
            err = item.Value(func (v []byte) error {
                return outInfo.Unmarshal(v)
            })
        }
        if err != nil {
            chSt.Errorf("entry %v load EntryInfo error: %v", inEntryID.Str(), err)  
        }
    }

    // EntryState
    if outState != nil && err == nil {

        outState.Flags = 0
        outState.Status = EntryStatus(0)
        outState.LiveIDs = outState.LiveIDs[:0]

        entryKey[chEntryTypePos] = chEntryStateKey
        item, err = chTxn.Get(entryKey[:])
        if err == nil {
            err = item.Value(func (v []byte) error {
                return outState.Unmarshal(v)
            })
        }
        if err != nil {
            chSt.Errorf("entry %v load EntryState error: %v", inEntryID.Str(), err)  
        }
    }

    // entry body
    if err == nil {
        if loadBody != nil {
            entryKey[chEntryTypePos] = chEntryBodyKey
            item, err = chTxn.Get(entryKey[:])
            if err == nil {
                err = item.Value(loadBody)
            }
            if err != nil {
                chSt.Errorf("entry %v load body error: %v", inEntryID.Str(), err)  
            }
        }
    }

    return err
}



func (chSt *ChStore) loadNextEntry(
    inTxn *badger.Txn,
    inPrevTID plan.TID,
    entry *chEntry,
) (bool, error) {

    var (
        entryKey plan.TIDBlob
    )

    chTxn := inTxn
    if chTxn == nil {
        chTxn = chSt.db.NewTransaction(false)
        defer chTxn.Discard()
    }

    haveEntry := chSt.seekToNextEntry(chTxn, inPrevTID, entryKey[:])
    if ! haveEntry {
        return false, nil
    }

    err := chSt.loadEntryHL(
        inTxn,
        entryKey[:], 
        entry)

    return true, err
}



func (chSt *ChStore) loadEntryHL(
    inTxn *badger.Txn,
    inEntryID plan.TID,
    entry *chEntry,
) error {

    entry.stateStatus = partNotLoaded
    entry.bodyStatus  = partNotLoaded
    entry.Body = entry.Body[:0]

    chTxn := inTxn
    if chTxn == nil {
        chTxn = chSt.db.NewTransaction(false)
        defer chTxn.Discard()
    }

    entry.Reset()
    err := chSt.loadEntry(
        inEntryID, 
        chTxn,
        &entry.Info,
        &entry.State,
        nil,
    )

    entry.StatePrev = entry.State
    entry.stateStatus = partLoaded

    return err

}


func (chSt *ChStore) seekToNextEntry(
    inTxn *badger.Txn,
    inPrevTID plan.TID,
    outEntryTID plan.TID,
) bool {

    chTxn := inTxn
    if chTxn == nil {
        chTxn = chSt.db.NewTransaction(false)
        defer chTxn.Discard()
    }

    // Increment to the the next entry
    if len(inPrevTID) >= 8 && bytes.Compare(inPrevTID, chEntryStart) > 0 {
        outEntryTID.CopyNext(inPrevTID)
    } else {
        copy(outEntryTID, chEntryStart)
    }

    opts := badger.IteratorOptions{}
    itr := chTxn.NewIterator(opts)
    defer itr.Close()

    itr.Seek(outEntryTID[:])
    if ! itr.Valid() {
        return false
    }

    copy(outEntryTID, itr.Item().Key())
    //chSt.Info(2, "next key: ", itr.Item().Key(), outEntryTID.SuffixStr())

    return true

}


// loadNextEntryToValidate reads the given entry from this ChStore that requires validation
func (chSt *ChStore) loadNextEntryToValidate(entry *chEntry) bool {

    // update chSt.State.ValidatedUpto
    // TODO: use badger steam read
    chSt.dequeueRevalidation()
 
    haveEntry, _ := chSt.loadNextEntry(
        nil, 
        chSt.State.ValidatedUpto,
        entry,
    )

    return haveEntry

}


func (chSt *ChStore) loadOriginalEntryBody(
    entry *chEntry,
    outBody ptools.Unmarshaller,
) error {

    var err error

    // Load the body if it hasn't been already
    if entry.bodyStatus == partNotLoaded {
        chTxn := chSt.db.NewTransaction(false)
        defer chTxn.Discard()

        err = chSt.loadEntry(
            entry.Info.EntryID(),
            chTxn,
            nil,
            nil,
            func(v []byte) error {
                entry.Body = append(entry.Body[:0], v...)
                return nil
            },
        )

        if err == nil {
            entry.bodyStatus = partLoaded
        }
    }

    if outBody != nil && err == nil {
        err = outBody.Unmarshal(entry.Body)
    }

    return err
}



// loadLatestEntryBody reads the given entry ID from this ChStore.
func (chSt *ChStore) loadLatestEntryBody(
    inTxn *badger.Txn,
    inEntryID plan.TID,
    outInfo *pdi.EntryInfo,
    outBody ptools.Unmarshaller,
) error {

  // If we didn't find the epoch already loaded, look it up in the db
    var (
        entryState EntryState
    )

    chTxn := inTxn
    if chTxn == nil {
        chTxn = chSt.db.NewTransaction(false)
        defer chTxn.Discard()
    }

    err := chSt.loadEntry(
        inEntryID, 
        chTxn,
        outInfo,
        &entryState,
        nil,
    )

    if err == nil {
        if entryState.Status == EntryStatus_LIVE {
            if outBody != nil {
                var liveBodyID plan.TID

                // A empty live body denotes to use the entry's own TID
                if len(entryState.LiveIDs) >= plan.TIDSz {
                    liveBodyID = entryState.LiveIDs[:plan.TIDSz]
                } else {
                    liveBodyID = inEntryID
                }

                var entryKey [chEntryKeySz]byte
                copy(entryKey[:], liveBodyID)
                entryKey[chEntryTypePos] = chEntryBodyKey

                // Fetch and unmarshal the body
                item, err := chTxn.Get(entryKey[:])
                if err == nil {
                    err = item.Value(func (val []byte) error {
                        return outBody.Unmarshal(val)
                    })
                }
            }
        } else {
            err = plan.Error(nil, plan.ChEntryIsNotLive, "entry not live")
        }
    }

    return err
}






func writeEntryItem(
    wb *badger.WriteBatch,
    entry *chEntry,
    entryID plan.TID,
    inKey byte, 
    n int, 
    valBuf []byte,
    v2 ptools.Marshaller,
) (int, error) {

    N := len(entry.scrap)
    if N - n < 1000 {
        N = 2000
        entry.scrap = make([]byte, N)
        n = 0
    }

// TODO makeover with bytes.Buffer!
    var err error
    if v2 != nil {
        sz := v2.Size()
        if n + sz < N {
            sz, err = v2.MarshalTo(entry.scrap[n:])
            valBuf = entry.scrap[n:n+sz]
            n += sz
        } else {
            valBuf, err = v2.Marshal()
        }
    }

    if err == nil {
        if entryID == nil {
            entryID = entry.Info.EntryID()
        }

        keyPos := n
        n += copy(entry.scrap[n:], entryID)
        entry.scrap[n] = inKey
        n++

        err = wb.Set(entry.scrap[keyPos:n], valBuf, 0)
    }

    return n, err
}



// flushEntry writes any dirty parts of chEntry to the channel db
func (chSt *ChStore) flushEntry(entry *chEntry) error {

    // Exit if there's no work to do
    if entry.stateStatus != partTouched && entry.bodyStatus != partTouched  && len(entry.Info.SupersedesEntryID) == 0 {
        return nil
    }

    wb := chSt.db.NewWriteBatch()

    var (
        err error
        n int
    )

    entryID := entry.Info.EntryID()

    // If we're merging, write out info and body entries (they only need to be written once)
    if entry.bodyStatus == partTouched {
        n, err = writeEntryItem(wb, entry, entryID, chEntryInfoKey, n, nil, &entry.Info)
        //chSt.Info(2, "wrote EntryInfo: ", entryID.SuffixStr())
        chSt.Log(err, "storing EntryInfo")
        
        if err == nil {
            n, err = writeEntryItem(wb, entry, entryID, chEntryBodyKey, n, entry.Body, nil)
            chSt.Log(err, "storing entry.Body")
        }

        // All channel epoch TIDs are stored as as a key only, so epoch history can be easily traversed
        if entry.Info.EntryOp == pdi.EntryOp_NEW_CHANNEL_EPOCH {
            keyPos := n
            n += copy(entry.scrap[n:], chEpochsPrefix)
            n += copy(entry.scrap[n:], entryID)
            err = wb.Set(entry.scrap[keyPos:n], nil, 0)
            chSt.Log(err, "storing entry channel epoch")
        }

        if err == nil {
            entry.bodyStatus = partLoaded
        }
    }

    // If this entry is now live and supercedes another, update the superceded entry's state to include the superceded entry ID.
    // An entry never places it's own entry ID in it's EntryState live IDs since it is implied.
    if len(entry.Info.SupersedesEntryID) > 0 {
        if entry.LivenessChanged() {
            var supersededState EntryState
            err = chSt.loadEntry(
                entry.Info.SupersedesEntryID,
                nil,
                nil, 
                &supersededState,
                nil,
            )

            if entry.IsLive() {
                supersededState.AddLiveID(entryID)
            } else {
                supersededState.StrikeLiveID(entryID)
            }

            n, err = writeEntryItem(wb, entry, entry.Info.SupersedesEntryID, chEntryStateKey, n, nil, &supersededState)
            chSt.Log(err, "storing superceded EntryState")
        }
    }

    if entry.stateStatus == partTouched && err == nil {
        n, err = writeEntryItem(wb, entry, entryID, chEntryStateKey, n, nil, &entry.State)
        chSt.Log(err, "storing EntryState")
        if err == nil {
            entry.stateStatus = partLoaded
        }
    }


    if err == nil {
        err = wb.Flush()
        chSt.Log(err, "flushing chEntry")
    } else {
        wb.Cancel()
    }

    if err == nil {
        if entry.bodyStatus == partTouched {
            entry.bodyStatus = partLoaded
        }
        if entry.stateStatus == partTouched {
            entry.stateStatus = partLoaded
        }
    }

    return err
}




// ReverseEntry -- see ChAgent.ReverseEntry
func (chSt *ChStore) ReverseEntry(
    inURID []byte,
) error {

    return nil

}


// OnLivenessChanged -- see ChAgent.OnLivenessChanged
func (chSt *ChStore) OnLivenessChanged(
    entry *chEntry,
) error {

    return nil

}


func (chSt *ChStore) FetchChEpoch(
   inEntryID plan.TID,
) *ChEpochNode {

    N := len(chSt.chEpochNodes)

    pos := sort.Search(N,
        func(i int) bool {
            return bytes.Compare(chSt.chEpochNodes[i].Epoch.EpochTID, inEntryID) >= 0
        },
    )

	if pos < N {
		epochNode := chSt.chEpochNodes[pos]
		if bytes.Equal(epochNode.Epoch.EpochTID, inEntryID) {
			return epochNode
		}
	}

    return nil
}


// GetActiveChEpoch returns the most recent and live channel epoch posted to this channel
func (chSt *ChStore) GetActiveChEpoch() *ChEpochNode {

    // Start from the most recent
    for i := len(chSt.chEpochNodes) - 1; i >= 0; i++ {
    	epochNode := chSt.chEpochNodes[i]
        if epochNode.Status == EntryStatus_LIVE {
            return epochNode
        }
    }

    return nil
}

/*

func (chSt *ChStore) FetchChEpoch(
   inEntryID plan.TID,
) *pdi.ChannelEpoch {

    epochID := inEntryID.Blob()

    // Is the epoch already loaded?
    chSt.epochMutex.RLock()
    chEpoch := chSt.epochCache[epochID]
    chSt.epochMutex.RUnlock()

    if chEpoch == nil {
        chEpoch = &pdi.ChannelEpoch{}
        var entryInfo EntryInfo

        err := chSt.loadLatestEntryBody(nil, inEntryID, &entryInfo, chEpoch)

        if err == nil {

            // Alternatively, we could write this when the entry is merged, but why waste space
            chEpoch.EpochTID = append(chEpoch.EpochTID[:0], inEntryID...)

            chSt.epochMutex.Lock()
            chSt.epochCache[epochID] = chEpoch
            chSt.epochMutex.Unlock()
        } else {
            chEpoch = nil
        }
    }

    return chEpoch

}
*/

// FetchAuthoringTIDs fills in the TIDs that a newly authored entry should use.
func (chSt *ChStore) FetchAuthoringTIDs(
   outChEpochTID plan.TID,
   outACCEntryTID plan.TID,
) error {

    var err error

    epochNode := chSt.GetActiveChEpoch()
    if epochNode == nil {
        return plan.Error(nil, plan.ChannelEpochNotFound, "failed to find live channel epoch")
    }

    copy(outChEpochTID, epochNode.Epoch.EpochTID)

    if epochNode.Epoch.HasACC() {
        acc, fetchErr := chSt.chMgr.FetchACC(epochNode.Epoch.ACC)
        if fetchErr == nil {
            accEpoch := acc.Store().GetActiveChEpoch()
            if accEpoch == nil {
                err = plan.Error(nil, plan.ChannelEpochNotFound, "failed to find live ACC epoch")
            }

            copy(outACCEntryTID, accEpoch.Epoch.EpochTID)
        } else {
            err = fetchErr
        }
    } else {
        copy(outACCEntryTID, plan.NilTID[:])
    }

    return err
}



// ValidateAgainstEpochHistory checks this channel's ChannelEpoch history to make sure the given entry is valid to go live.
func (chSt *ChStore) ValidateAgainstEpochHistory(
    entry *chEntry,
) (epochNode *ChEpochNode, err error) {

    checkEntry := true

    if entry.Info.EntryOp == pdi.EntryOp_NEW_CHANNEL_EPOCH {

        // If this entry is creating a new channel (and contains the genesis channel epoch), only the genesis entry needs to be validated.
        // Otherwise, new channel epoch posts still require that the channel accepts it (below).
        if entry.HasFlag(EntryFlag_IS_CHANNEL_GENESIS) {
            checkEntry = false
        }
    }

    // Does this entry comply with this channel's epoch history
    if checkEntry && err == nil {

        chEpochID := entry.Info.ChannelEpochID()

        epochNode = chSt.FetchChEpoch(chEpochID)
        if epochNode == nil {
            return nil, plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v not found", chEpochID)
        }
        if epochNode.Status != EntryStatus_LIVE {
            return nil, plan.Errorf(nil, plan.ChannelEpochNotLive, "channel epoch %v is not live", chEpochID)
        }
        if len(entry.Info.SupersedesEntryID) > 0 && ! epochNode.Epoch.EntriesSupersedable {
            return nil, plan.Errorf(nil, plan.ChannelEpochDisallows, "channel epoch does not allow entries ot be superseded")
        }

        timeAuthored := entry.Info.TimeAuthored()

        // This will be reworked in the future for full correctness, but for now there's placeholder logic to demonstrate what should be checked.
        for _, next := range epochNode.Next {
            if next.Status == EntryStatus_LIVE {
                successorTime := plan.TID(next.Epoch.EpochTID).ExtractTime()
                if timeAuthored > successorTime + next.Epoch.EpochTransitionPeriod {
                    return nil, plan.Errorf(nil, plan.ChannelEpochExpired, "channel epoch %v has been superseeded by %v", plan.TID(epochNode.Epoch.EpochTID).Str(), plan.TID(next.Epoch.EpochTID).Str())
                }
            }
        }
    }

    return epochNode, nil
}


// IsEntryAuthorized returns no error only if the cited channel epoch (and implied epoch chain) authorizes this entry.
func (chSt *ChStore) IsEntryAuthorized(
    inCitedEpoch plan.TID,
    entry *chEntry,
) error {

    //entryStatus := EntryStatus_AWAITING_VALIDATION

    epoch := chSt.FetchChEpoch(inCitedEpoch)
    if epoch == nil {
        return plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v in acc %v not found", entry.Info.ChannelEpochID(), chSt.State.ChannelID)
    }

    // Normal permissions lookup. boring: do last :\
    { }

    /*
    if epoch.Extensions != nil {
        epoch.Extensions.FindBlocksWithCodec(codecChEntryWhitelistURID, 0, func(inMatch *plan.Block) error {
            if bytes.Equal(inEntryURID, inMatch.Content) {
                entryStatus = EntryStatus_LIVE
            }
            return nil
        })
    }*/



    return nil

}



// MergePost -- see ChAgent.MergePost
func (chSt *ChStore) MergePost(
    entry *chEntry,
) error {

    return nil
}



// MergeNewChannelEpoch -- see ChAgent.MergeNewChannelEpoch
func (chSt *ChStore) MergeNewChannelEpoch(
    entry *chEntry,
) error {


    plan.Assert(entry.Info.EntryOp == pdi.EntryOp_NEW_CHANNEL_EPOCH, "expected EntryOp_NEW_CHANNEL_EPOCH")
    
    return nil
}




/*
// TxnHelper wraps ds.Txn for easier Commit() reattempts
type ChStoreTxn struct {
	Txn         *badger.Txn

	// Private
	maxAttempts int
	readOnly    bool
	isDone      bool
	commitErr   error
	fatalErr    error
	tryNum      int
}


func (tx *ChStoreTxn) NewTxn(bool inReadWrite) ChStoreTxn {
	return ChStoreTxn{
		db:          db,
		maxAttempts: 5,
		readOnly:    false,
		tryNum:      0,
	}
}



// NewTxnHelper returns a helper struct that wraps datastore.Txn for convenience.
// This makes life easier for reattempting txns if they go stale, etc.
func NewTxnHelper(db *badger.DB) TxnHelper {
	return TxnHelper{
		db:          db,
		maxAttempts: 5,
		readOnly:    false,
		tryNum:      0,
	}
}

// NextAttempt is intended to be used to control a for loop and returns true only if:
//     1) the max number of attempts has not been reached
//     2) Finish() has not been called with a non-nil error
//     3) the txn has not yet been committed successfully yet.
func (h *TxnHelper) NextAttempt() bool {
	if h.Txn != nil {
		panic("BeginTry/EndTry mismatch")
	}

	if h.fatalErr != nil || h.isDone {
		return false
	}

	if h.tryNum >= h.maxAttempts {
		h.fatalErr = plan.Errorf(h.commitErr, plan.StorageNotReady, "datastore txn retry limit exceeded (%v)", h.maxAttempts)
		return false
	}

	h.Txn = h.db.NewTransaction(!h.readOnly)

	h.tryNum++
	return true
}

// Finish is called with inFatalErr == nil to denote that the ds.Txn should be committed.  If the commit
//     succeeds, the next call to NextAttempt() will have no effect and return false.  If the commit
//     fails (b/c the txn got stale), NextAttempt() will return true until the max number of attempts has been reached.
// If inFatalErr != nil, then the current ds.Txn.Discard() is called  and the next call to NextAttempt() will return false.
func (h *TxnHelper) Finish(inFatalErr error) {

	// Note, this will overwrite any prev set fatal err
	if inFatalErr != nil {
		h.fatalErr = inFatalErr
	}

	if h.Txn != nil {

        if inFatalErr == nil {
            err := h.Txn.Commit()
            if err == nil {
                h.isDone = true
            } else {
                h.commitErr = plan.Error(err, plan.StorageNotReady, "badger.Txn.Commit() failed")
            }
        } else {
            h.Txn.Discard()
        }
        
		h.Txn = nil
	}
}

// FatalErr returns the most recent error passed to Finish() *or* an error reflecting that the max number of retries was reached.
// If non-nil, this reflects that the txn was NOT committed.
func (h *TxnHelper) FatalErr() error {
	return h.fatalErr
}
*/

const chEntryScrapSz = 256
const chEntryStateScrapSz = 32
const chDepScrapSz = 64

// ValidateEntry checks that the conditions needed for an entry to go live. 
func (chSt *ChStore) ValidateEntry(
    entry *chEntry,
) {

    var (
        valErr error
    )

    if ! entry.HasFlags(EntryFlag_GENESIS_ENTRY_VERIFIED, EntryFlag_WELL_FORMED) {
    
        /*****************************************************
        ** 1 -- Check community membership records
        **/
        {
            registry, err := chSt.chMgr.FetchMemberRegistry()
            if err == nil {
                
                // Write the dependency so this entry will be revalidated if the dep changes liveness.
                if ! entry.HasFlags(EntryFlag_AUTHOR_DEPENDENCY_WRITTEN) {
                    err = registry.AddDependency(entry.Info.AuthorEntryID(), chSt.ChID(), &entry.Info)
                    if err == nil {
                        entry.AddFlags(EntryFlag_AUTHOR_DEPENDENCY_WRITTEN)
                    }
                }
                if err == nil {
                    err = registry.ValidateAuthor(entry)
                }
            }

            // If we can't validate the author (including failing to write the dependency), don't allow the entry to go live.
            if err != nil {
                valErr = err
            }
        }

        var epochNode *ChEpochNode

        /*****************************************************
        ** 2 -- Check entry's cited channel epoch
        **/
        if valErr == nil {
  
            epochNode, valErr = chSt.ValidateAgainstEpochHistory(entry)

            // Note how no dependency is needed for the referenced channel epoch b/c if/when a channel entry is overturned, 
            //    all subsequent entries in that channel always undergo revalidation,
                
        }

        /*****************************************************
        ** 3 -- Check that the entry has permission to post
        **/
        if valErr == nil {
            accEntryID := entry.Info.ACCEntryID()

            var err error

            if entry.HasFlag(EntryFlag_IS_CHANNEL_GENESIS) {

                // No op during chanel genesis since ACC authority is not used/needed.  

            } else if accEntryID.IsNil() {

                // If the entry does not cite an ACC epoch, then the cited channel epoch is expected to perform authorization.
                err = chSt.IsEntryAuthorized(entry.Info.ChannelEpochID(), entry)
            } else { 
                chEpochHasACC := epochNode != nil && epochNode.Epoch.HasACC()

                // If the entry cites an ACC epoch, then fetch the ACC and use that for authorization.
                if chEpochHasACC {
                    var acc ChAgent
                    acc, err = chSt.chMgr.FetchACC(epochNode.Epoch.ACC)
                    if err == nil {

                        // Write the ACC dependency so that we know to revalidate this entry if the cited entry changes
                        if ! entry.HasFlags(EntryFlag_ACC_DEPENDENCY_WRITTEN) {
                            err = acc.Store().AddDependency(accEntryID, chSt.ChID(), &entry.Info)
                            if err == nil {
                                entry.AddFlags(EntryFlag_AUTHOR_DEPENDENCY_WRITTEN)
                            }
                            if err == nil {
                                err = acc.Store().IsEntryAuthorized(accEntryID, entry)
                            }
                        }
                    } else {
                        err = plan.Error(err, plan.ACCNotFound, "cited ACC in ch epoch not accessible")
                    }
                } else {
                    err = plan.Error(nil, plan.ChEntryIsMalformed, "entry references ACC epoch but cited channel epoch has no ACC set")
                    entry.ThrowMalformed(err)
                }
            }

            if err != nil {
                valErr = err
            }
        }

    }

    // Entry is deferred by default
    status := EntryStatus_DEFERRED
    if entry.IsWellFormed() {
        if valErr == nil {
            status = EntryStatus_LIVE
        }
    } else {
        status = EntryStatus_DISBARRED
    }

    entry.SetStatus(status)

    if (entry.LivenessChanged() || entry.stateStatus == partTouched) && chSt.LogV(1) {
        var errStr string 
        if valErr != nil {
            errStr = fmt.Sprintf(" (%v)", valErr)
        }

        chSt.Info(1, "entry ", entry.Info.EntryID().SuffixStr(), " => ", gEntryStatusDesc[entry.State.Status], errStr)
    }
}

var gEntryStatusDesc = map[EntryStatus]string{
	EntryStatus_DISBARRED: "\x1b[31mDISBARRED\x1b[0m",
	EntryStatus_AWAITING_MERGE: "\x1b[37mAWAITING_MERGE\x1b[0m",
	EntryStatus_MERGED: "\x1b[36mMERGED\x1b[0m",
	EntryStatus_DEFERRED: "\x1b[34mDEFERRED\x1b[0m",
	EntryStatus_LIVE: "\x1b[32mLIVE\x1b[0m",
}


// AddDependency adds the given dependency, storing a state such that subsequent entries posted to reverted to this channel
//    can cause dependent channels to commence relvalidation. 
func (chSt *ChStore) AddDependency(
    inRefdTID  plan.TID,
    inDepChID  plan.ChID,
    inDepEntry *pdi.EntryInfo,
) error {

    var (
        depKey [chEntryDepKeySz]byte
        chDep ChDependency
    )

    plan.Assert(len(inDepChID) == plan.ChIDSz, "bad channel ID")

    copy(depKey[:], inRefdTID)
    depKey[plan.TIDSz] = chEntryDepsKey
    copy(depKey[chEntryKeySz:], inDepChID)

    // Typically, we won't need to write but we have to do it anyway in the event that we do.
    chTxn := chSt.NewWrite()
    needsUpdate := false

    depTime := int64(inDepEntry.TimeAuthoredFS())

    // If there's already a dependency for the referenced dep key in the given channel, 
    //    choose the more restrictive dependency.
    item, err := chTxn.Get(depKey[:])
    if err == nil {
        err = item.Value(func(val []byte) error {
            return chDep.Unmarshal(val)
        })
        if err == nil {
            if depTime < chDep.DepTime {
                needsUpdate = true
            }
        }
    } else if err == badger.ErrKeyNotFound {
        err = nil
        needsUpdate = true
    }

    if err == nil && needsUpdate {
        chDep.DepTime = depTime
        var scrap [chDepScrapSz]byte
        len, err := chDep.MarshalTo(scrap[:])
        if err != nil {
            panic(err)
        }

        err = chTxn.Set(depKey[:], scrap[:len])
        if err != nil {
            panic(err)
        }

        err = chTxn.Commit()
        if err != nil {
            // TODO: handle failed commit.  When exactly can commits fail??
        }

    } else {
        chTxn.Discard()
    }

    return err
}

// RevalidateDependencies queues revalidation for all possible dependencies for the given entry.
//
// Pre: inChEntry has been posted to this channel.
func (chSt *ChStore) RevalidateDependencies(
    chEntry *pdi.EntryInfo,
) {

    
    var (
        searchKey [chEntryKeySz]byte
        chDep ChDependency
        //depTypes [3]byte
        //scrap [1 + 8]byte
        //chTxn *badger.Txn
    )



    // First, if the entry could affect subsequent entries WITHIN this channel, queue revalidation
    switch chEntry.EntryOp {
        case pdi.EntryOp_NEW_CHANNEL_EPOCH:
            chSt.QueueRevalidation(chEntry.TimeAuthoredFS() /* - communityEpoch.MaxMemberClockDelta? */)
    }


    copy(searchKey[:], chEntry.EntryID())
    searchKey[plan.TIDSz] = chEntryDepsKey

    //searchKey := inChEntry.SetupDepSearchKey(scrap)

    // If we find a match, queue the revalidation
    chTxn := chSt.db.NewTransaction(false)
 
    opts := badger.DefaultIteratorOptions
    opts.PrefetchValues = false
    opts.Prefix = searchKey[:]
    itr := chTxn.NewIterator(opts)

    // Make a new iterator, search for a dep key match for each type of dependency
    {

        // Iterate thru all channel dependencies that reference this entry by this entry's entry ID
        for itr.Seek(searchKey[:]); itr.Valid(); itr.Next() {
            item := itr.Item()

            itemKey := item.Key()
            
            depChID := itemKey[chEntryKeySz:]
                
            err := item.Value(func(v []byte) error {
                return chDep.Unmarshal(v)
            })
            if err != nil {
                chSt.chMgr.CtxOnFault(err, "loading ch dep")
            } else {
                depCh, _ := chSt.chMgr.FetchChannel(depChID)
                if depCh == nil {
                    // TODO: handle very rare case where a channel this is momentarily shutdown.
                    // Maybe just keep sleepping for a few ms until its non-nil?
                    {}
                } else {
                    depCh.Store().QueueRevalidation(plan.TimeFS(chDep.DepTime))
                }
            }

        }
    }

    itr.Close()
    chTxn.Discard()

}

// QueueRevalidation queues all entries on or after the given time index for revalidation.
func (chSt *ChStore) QueueRevalidation(inAfterTime plan.TimeFS) {

    chSt.Infof(1, "QueueRevalidation to %v", inAfterTime)

    chSt.revalRequestMutex.Lock()
    if inAfterTime < chSt.revalAfter {
        chSt.revalAfter = inAfterTime
        chSt.Infof(1, "revalAfter = %v", inAfterTime)
    }
    chSt.revalRequestMutex.Unlock()
}

// dequeueRevalidation flushes queued revalidation messages with the current reval state.
func (chSt *ChStore) dequeueRevalidation() {

    if len(chSt.State.ValidatedUpto) != plan.TIDSz {
        chSt.State.ValidatedUpto = make([]byte, plan.TIDSz)
    }

    valUpto := plan.TID(chSt.State.ValidatedUpto)

    chSt.revalRequestMutex.Lock()
    if valUpto.SelectEarlier(chSt.revalAfter) {
        chSt.Infof(1, "rewinding re-validation head to %v", chSt.revalAfter)
    }
    chSt.revalAfter = plan.TimeFSMax
    chSt.revalRequestMutex.Unlock()

    // TODO: flush to disk so a crash doesn't cause validation state to get dropped

}







func mergeEntry(
    ch ChAgent,
    entry *chEntry,
) error {
    chSt := ch.Store()

    var err error

    if ! chSt.State.MergeEnabled {
        // entry.entryAsset = entry.Body
    } else {
        switch entry.Info.EntryOp {
            case pdi.EntryOp_NEW_CHANNEL_EPOCH:
                err = ch.MergeNewChannelEpoch(entry)
            case pdi.EntryOp_POST_CONTENT:
                err = ch.MergePost(entry)
        }
        if plan.IsError(err, plan.ChEntryIsMalformed) {
            entry.ThrowMalformed(err)
        } else if err == nil {
            entry.SetStatus(EntryStatus_MERGED)
        }

        // TODO: how do we handle malformed entries from here?
        { }
    }

    return err
}


func startupChannel(ch ChAgent) error {

    chSt := ch.Store()

    err := chSt.loadChEpochs()
    if err != nil {
        panic("error loading ch epochs")
    }

    err = ch.Startup()

    if err == nil {

        if chSt.processingEntries {
            panic("channel not in startup state")
        }

        go chEntryProcessor(ch)
    }

    return err
}




func chEntryProcessor(ch ChAgent) {

    chSt := ch.Store()

    var (
        shuttingDown bool
        entryTmp *chEntry
    )

    moreToValidate := true

    chSt.processingEntries = true
    chSt.chShutdown.Add(1)

    for {

        var (
            entry *chEntry
        )

        // Prioritize entries that are ready to merge over entries pending validation
        select {
            case entry = <- chSt.entriesToMerge:
                if entry == nil {
                    shuttingDown = true
                }
            default:
                // If we're not told anything new, keep validating deferred entries!
        }

        // Only block when we have no entry to merge AND we're up to date with validation
        if entry == nil && ! moreToValidate && ! shuttingDown {
            if entryTmp != nil {
                RecycleChEntry(entryTmp)
                entryTmp = nil
            }
            entry = <- chSt.entriesToMerge
            if entry == nil {
                shuttingDown = true
            }
        }

        // Sending a nil entry value is a signal to shutdown this channel
        if shuttingDown {
            break
        }
        
        verboseLog := chSt.LogV(1)

        {
            var logInfo string

            // Do initial merge or load the ChEntry from the chDB
            if entry != nil {
                logInfo =  "merging"
            } else if ! chSt.State.MergeEnabled {
                moreToValidate = false
            } else {
                if entryTmp == nil {
                    entryTmp = NewChEntry(entryRevalidating)
                }

                moreToValidate = chSt.loadNextEntryToValidate(entryTmp)
                if moreToValidate {
                    entry = entryTmp

                    logInfo =  "validating"

                    // If we still haven't merged yet, we need to load the body, as if it just arrived chSt.entriesToMerge.
                    // Since this only occurs when chSt.State.MergeEnabled == false later turns true, this is rare and does not have to be efficient
                    if entry.State.Status == EntryStatus_AWAITING_MERGE {
                        chSt.loadOriginalEntryBody(entry, nil)
                    }
                } else {
                    chSt.Info(1, "validation complete") 
                }
            }

            if entry == nil {
                continue
            }
            
            if verboseLog {
                chSt.Infof(1, "%s entry %s (%s, %s)", logInfo, entry.Info.EntryID().SuffixStr(), pdi.EntryOp_name[int32(entry.Info.EntryOp)], EntryStatus_name[int32(entry.State.Status)])
            }

            var err error

            // Does the entry need to be merged?
            if entry.State.Status == EntryStatus_AWAITING_MERGE {
                err = mergeEntry(ch, entry)
                chSt.chMgr.CtxOnFault(err, "merging entry")
            }

            switch entry.State.Status {
                case EntryStatus_MERGED:      fallthrough 
                case EntryStatus_DEFERRED:    fallthrough 
                case EntryStatus_LIVE:
                    chSt.ValidateEntry(entry)
            }

            // flush any changes to the entry to the channel db
            err = chSt.flushEntry(entry)

            if entry.onMergeComplete != nil {
                entry.onMergeComplete(entry, ch, err)
                entry.onMergeComplete = nil
            }

            if err != nil {

                // TODO: handle entry.txnSet
                { }

            } else {

                if entry.origin == entryFromStorageProvider || entry.origin == entryWasAuthored {
                    chSt.chMgr.CR.txnsToWrite <- entry.PayloadTxnSet
                    entry.PayloadTxnSet = nil
                }

                if entry.origin == entryRevalidating {
                    copy(chSt.State.ValidatedUpto, entry.Info.EntryID())
                }

                // If the status of this entry has changed, revalidate channels that are known to be dependent.
                if entry.LivenessChanged() {
                    chSt.onLivenessChangedInternal(entry)

                    chSt.RevalidateDependencies(&entry.Info)

                    ch.OnLivenessChanged(entry)
                }
            }

            if entry != entryTmp {
                RecycleChEntry(entry)
                entry = nil
            }
        }

    }

    chSt.chShutdown.Done()
}


func chSessionEntryReader(cs *ChSession) {

    const batchMax = 10

    chSt := cs.Agent.Store()

    var (
        includeDeferred,includeBody,includeContent, includeChEpochs bool
    )

    autoReading := false

    for cs.CtxRunning() {

        var (
            entry chEntry
            body ptools.Buf
            curPos, updatedEntry plan.TIDBlob
            chTxn *badger.Txn
        )

        batchCount := 0

        // Scan a batch
        for cs.CtxRunning() || batchCount < batchMax {

            gotUpdate := false

            var readerCmd uint32
            if len(cs.readerCmdQueue) > 0 {
                readerCmd = <- cs.readerCmdQueue
            } else if len(cs.entryUpdates) > 0 {
                updatedEntry, gotUpdate = <- cs.entryUpdates
            } else if ! autoReading {
                select {
                    case updatedEntry, gotUpdate = <- cs.entryUpdates:
                    case readerCmd = <- cs.readerCmdQueue:
                }
            }


            if readerCmd != 0 {
                flags := readerCmd
                includeDeferred = 0 != (flags & (uint32(1)<<byte(ChSessionFlags_DEFERRED_ENTRIES)))
                includeBody     = 0 != (flags & (uint32(1)<<byte(ChSessionFlags_INCLUDE_BODY)))
                includeContent  = 0 != (flags & (uint32(1)<<byte(ChSessionFlags_CONTENT_ENTRIES)))
                includeChEpochs = 0 != (flags & (uint32(1)<<byte(ChSessionFlags_NEW_EPOCH_ENTRIES)))

                curPos.TID().SetTimeFS(0)
                autoReading = true
                // Stop this batch and restart
                break

            }
            
            if gotUpdate {
                // Ignore the updated entry if it's ahead of the current read pos
                if autoReading && bytes.Compare(updatedEntry.TID(), curPos.TID()) >= 0 {
                    gotUpdate = false
                }
                
                if chTxn != nil {
                    chTxn.Discard()
                    chTxn = nil
                }
            }

            if chTxn == nil {
                chTxn = chSt.db.NewTransaction(false)
            }

            gotEntry := false
            var err error

            var next plan.TID
            if gotUpdate {
                next = updatedEntry.TID()

                err = chSt.loadEntryHL(
                    chTxn,
                    next,
                    &entry)

                gotEntry = err == nil

            } else if autoReading {
                next = curPos.TID()

                gotEntry, err = chSt.loadNextEntry(
                    chTxn,
                    next,
                    &entry,
                )

                if gotEntry {
                    copy(curPos[:], entry.Info.EntryID())
                } else {
                    autoReading = false
                }

            }

            if err != nil {
                chSt.Errorf("read entry failed for entry %v: %v", entry.Info.EntryID().Str(), err)
                continue
            }

            if ! gotEntry {
                break
            }


            send := true

            switch entry.State.Status {
                case EntryStatus_LIVE:
                case EntryStatus_DEFERRED:
                    if ! includeDeferred {
                        send = false
                    }
                default:
                    send = false
            }

            switch entry.Info.EntryOp {
                case pdi.EntryOp_POST_CONTENT:
                    if ! includeContent {
                        send = false
                    }
                case pdi.EntryOp_NEW_CHANNEL_EPOCH:
                    if ! includeChEpochs {
                        send = false
                    }
            }

            if send && err == nil {
                if includeBody {
                    err = chSt.loadOriginalEntryBody(&entry, &body)
                }
                
                if err != nil {
                    chSt.Errorf("load body failed for entry %v: %v", entry.Info.EntryID().Str(), err)
                } else {
                    cs.MemberSession.msgOutbox <- &Msg{
                        Op: MsgOp_CH_ENTRY,
                        ChSessID: uint32(cs.ChSessID),
                        EntryInfo: entry.Info.Clone(),
                        EntryState: entry.State.Clone(),
                        BUF0: append([]byte{}, body.Bytes...),
                    }
                }
            }
        }

        if chTxn != nil {
            chTxn.Discard()
        }

    }

}

/*

            for j := i; j < N; j++ {
                state.SupersededURIDs[j] = state.SupersededURIDs[j+1]
            }
            state.SupersededURIDs = state.SupersededURIDs[:N]
        }
    }

    if bytes.Equal(inURID, state.LiveURID) {
        changed = true
        if N == 0 {
            state.LiveURID = state.LiveURID[:0]
        } else {
            state.LiveURID = state.SupersededURIDs[0]
            state.SupersededURIDs = state.SupersededURIDs[1:]
        }
    } else {

        for i := 0; i < N; i++ {
            if bytes.Equal(inURID, state.SupersededURIDs[i]) {
                N--
                changed = true
                for j := i; j < N; j++ {
                    state.SupersededURIDs[j] = state.SupersededURIDs[j+1]
                }
                state.SupersededURIDs = state.SupersededURIDs[:N]
            }
        }
    }
}



func (state *ChEntryLiveness) RemoveURID(
    inURID []byte,
) bool {

    changed := false

    N := len(state.supersededURIDs)

    if inStatus == EntryStatus_LIVE {

    } else {
        if bytes.Equal(inURID, state.LiveURID) {
            changed = true
            if N == 0 {
                state.LiveURID = state.LiveURID[:0]
            } else {
                state.LiveURID = state.SupersededURIDs[0]
                state.SupersededURIDs = state.SupersededURIDs[1:]
            }
        } else {
    
            for i := 0; i < N; i++ {
                if bytes.Equal(inURID, state.SupersededURIDs[i]) {
                    N--
                    for j := i; j < N; j++ {
                        state.SupersededURIDs[j] = state.SupersededURIDs[j+1]
                    }
                    state.SupersededURIDs = state.SupersededURIDs[:N]
                }
            }
        }
    }

}

*/

                


func init() {
        
    gChAgentRegistry[ChProtocolACC] = func(inChProtocol string) ChAgent {
        return &ChACC{}
    }
    gChAgentRegistry[ChProtocolMemberRegistry] = func(inChProtocol string) ChAgent {
        return &ChMemberRegistry{
            epochLookup: map[plan.TIDBlob]*pdi.MemberEpoch{},
        }
    }
    gChAgentRegistry[ChProtocolTalk] = func(inChProtocol string) ChAgent {
        return &ChTalk{}
    }
    gChAgentRegistry[ChProtocolSpace] = func(inChProtocol string) ChAgent {
        return &ChSpace{}
    }
    gChAgentRegistry[ChProtocolCommunityEpochs] = func(inChProtocol string) ChAgent {
        return &ChCommunityEpochs{
        }
    }
}

// ChUnknown -- ChAgent for a protocol not recognized or not yet known.
type ChUnknown struct {
    ChStore

}


// MergePost -- see ChAgent.MergePost
func (ch *ChUnknown) MergePost(
    entry *chEntry,
) error {

    return plan.Error(nil, plan.AssertFailed, "ChUnknown cannot merge entries")
}



// ChACC -- ChAgent for ChProtocolACC 
type ChACC struct {
    ChStore

}



// MergePost -- see ChAgent.MergePost
func (acc *ChACC) MergePost(
    entry *chEntry,
) error {

    return nil
}



/*
const (
    MemberEpochCodec = ChProtocolMemberRegistry + "/MemberEpoch"
)
*/
var (
    epochNotFound = &pdi.MemberEpoch{}
)

/* 
ChMemberRegistry -- ChAgent for ChProtocolMemberRegistry 
*/
type ChMemberRegistry struct {
    ChStore

    lookupMutex         sync.RWMutex
    epochLookup         map[plan.TIDBlob]*pdi.MemberEpoch

}




// MergePost -- see ChAgent.MergePost
func (ch *ChMemberRegistry) MergePost(entry *chEntry) error {

    epoch := pdi.MemberEpoch{}
    err := epoch.Unmarshal(entry.Body)
    if err != nil {
        return plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling ChMemberRegistry MemberEpoch")
    }

    return err
}


func (ch *ChMemberRegistry) ValidateAuthor(
    entry *chEntry,
) error {
     
    memberEpoch, err := ch.FetchMemberEpoch(entry.Info.AuthorEntryID())
    if err != nil {
        return err
    }

    // TODO: validate entry timestamp
    { }

    // Internally in a repo, this stores the pub key of signer of the final/packaged entry which must be compared to the key stored in the cited 
    if ! bytes.Equal(memberEpoch.PubSigningKey, entry.Info.AuthorSig) {
        return plan.Error(nil, plan.FailedToProcessPDIHeader, "author signature verification failed")
    }

    return nil
}

// FetchMemberEpoch returns the MemberEpoch associated with the given entry ID
func (ch *ChMemberRegistry) FetchMemberEpoch(inEntryID plan.TID) (*pdi.MemberEpoch, error) {

    entryID := inEntryID.Blob()

    // Is the epoch already loaded?
    ch.lookupMutex.RLock()
    memEpoch := ch.epochLookup[entryID]
    ch.lookupMutex.RUnlock()

    // TODO: check that the epoch is live!
    var err error

    if memEpoch == nil {
        memEpoch = &pdi.MemberEpoch{}

        err := ch.loadLatestEntryBody(nil, inEntryID, nil, memEpoch)

        if err == nil {

            // Alternatively, we could write this when the entry is merged, but why waste space
            memEpoch.EpochTID = append(memEpoch.EpochTID[:0], inEntryID...)

            ch.lookupMutex.Lock()
            ch.epochLookup[entryID] = memEpoch
            ch.lookupMutex.Unlock()
        }
    }

    if memEpoch == nil && err == nil {
        err = plan.Error(nil, plan.MemberEpochNotFound, "member epoch not found")
    }

    if err != nil {
        return nil, err
    }

    return memEpoch, nil

}


type ChEpochNode struct {

    Prev    *ChEpochNode
    Next    []*ChEpochNode

    Epoch   pdi.ChannelEpoch
    Status  EntryStatus

}


func (chSt *ChStore) loadChEpochs() error {

    var (
        err error
        epochTID plan.TIDBlob
        entryState EntryState
    )

    chTxn := chSt.db.NewTransaction(false)
    defer chTxn.Discard()

    opts := badger.IteratorOptions{
        Prefix: chEpochsPrefix,
    }

    itr := chTxn.NewIterator(opts)
    defer itr.Close()


    chSt.chEpochNodes = chSt.chEpochNodes[:0]

    for itr.Seek(nil); itr.Valid(); itr.Next() {
        itemKey := itr.Item().Key()
        copy(epochTID[:], itemKey[len(chEpochsPrefix):])

        node := &ChEpochNode{}

        err = chSt.loadEntry(
            epochTID[:], 
            chTxn,
            nil, 
            &entryState,
            func (v []byte) error {
                return node.Epoch.Unmarshal(v)
            },
        )

        if err == nil {
            node.Status = entryState.Status
            chSt.chEpochNodes = append(chSt.chEpochNodes, node)
        }
    }

    // Now that all the epochs are loaded (in TID order--no sorting needed), link them
    for _, node := range chSt.chEpochNodes {
        chSt.linkChEpochNode(node)
    }

    return nil

}

func (chSt *ChStore) onLivenessChangedInternal(entry *chEntry) {

    if entry.Info.EntryOp == pdi.EntryOp_NEW_CHANNEL_EPOCH {

        node := chSt.FetchChEpoch(entry.Info.EntryID())

        if node == nil {
            node = &ChEpochNode{}
            err := chSt.loadOriginalEntryBody(entry, &node.Epoch)
            chSt.Log(err, "failed to load ch epoch")

            if err == nil {
                // Append the new node and resort the them (so FetchChEpoch works)
                chSt.chEpochNodes = append(chSt.chEpochNodes, node)
                sort.Sort(ByEpochTID(chSt.chEpochNodes))

                // Link the newly added node
                chSt.linkChEpochNode(node)
            }
        }

        // Propigate the entry status to the epoch instance
        node.Status = entry.State.Status
    }

    // TODO: pass entry and use AddRef/ReleaseRef()?
    chSt.chSessionsMutex.RLock()
    for _, cs := range chSt.chSessions {
        cs.entryUpdates <- entry.Info.EntryID().Blob()
    }
    chSt.chSessionsMutex.RUnlock()

}


func (chSt *ChStore) linkChEpochNode(ioNode *ChEpochNode) {
    prev := chSt.FetchChEpoch(ioNode.Epoch.PrevEpochTID)
    if prev != nil {
        ioNode.Prev = prev
        prev.Next = append(prev.Next, ioNode)
    }
}


// ByEpochTID implements sort.Interface to sort a slice of ChEpochNodes by TID
type ByEpochTID []*ChEpochNode

func (a ByEpochTID) Len() int           { return len(a) }
func (a ByEpochTID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByEpochTID) Less(i, j int) bool { return bytes.Compare(a[i].Epoch.EpochTID, a[j].Epoch.EpochTID) < 0 }



type ChCommunityEpochs struct {
    ChStore

    lookupMutex         sync.RWMutex
    epochHistory        []*pdi.CommunityEpoch
}


func (ch *ChCommunityEpochs) Startup() error {

    var (
        entry chEntry
        err error
    )

    chTxn := ch.db.NewTransaction(false)
    defer chTxn.Discard()

    // Scan all entries from the start
    for {


        // TODO: handle error
        haveEntry, loadErr := ch.loadNextEntry(
            chTxn,
            entry.Info.EntryID(),
            &entry,
        )

        if ! haveEntry {
            break
        }

        if entry.State.Status != EntryStatus_LIVE {
            continue
        }

        if entry.Info.EntryOp != pdi.EntryOp_POST_CONTENT {
            continue
        }

        commEpoch := &pdi.CommunityEpoch{}

        ch.loadOriginalEntryBody(&entry, commEpoch)

        if loadErr == nil {
            ch.epochHistory = append(ch.epochHistory, commEpoch)
        }
    }

    return err
}


// OnLivenessChanged -- see ChAgent.OnLivenessChanged
func (ch *ChCommunityEpochs) OnLivenessChanged(
    entry *chEntry,
) error {

    if entry.Info.EntryOp == pdi.EntryOp_POST_CONTENT {

        entryID := entry.Info.EntryID()

        if entry.IsLive() {

            commEpoch := &pdi.CommunityEpoch{}

            err := ch.loadEntry(
                entryID,
                nil,
                nil,
                nil,
                func(v []byte) error {
                    return commEpoch.Unmarshal(v)
                },
            )

            if err == nil {
                ch.epochHistory = append(ch.epochHistory, commEpoch)
            }

        } else {
            N := len(ch.epochHistory)
            for i := 0; i < N; i++ {
                epoch := ch.epochHistory[i]
                if bytes.Equal(epoch.EpochTID, entryID) {
                    N--
                    ch.epochHistory[i] = ch.epochHistory[N]
                    ch.epochHistory = ch.epochHistory[:N]
                }
            } 
            
        }
    }

    return nil

}



// MergePost -- see ChAgent.MergePost
func (ch *ChCommunityEpochs) MergePost(entry *chEntry) error {

    epoch := pdi.CommunityEpoch{}
    err := epoch.Unmarshal(entry.Body)
    if err != nil {
        return plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling ChMemberRegistry MemberEpoch")
    }

    epoch.EpochTID = entry.Info.EntryID()
    entry.Body, err = epoch.Marshal()
    entry.bodyStatus = partTouched
    
    return err
}


func (ch *ChCommunityEpochs) LatestCommunityEpoch() *pdi.CommunityEpoch {

    var (
        latest *pdi.CommunityEpoch 
        latestTime plan.TimeFS
    )

    // TODO: Sort my time issued
    for _, epoch := range ch.epochHistory {
        epochTime :=  plan.TID(epoch.EpochTID).ExtractTimeFS()
        if epochTime >= latestTime {
            latestTime = epochTime
            latest = epoch
        }
    }

    return latest
}



func (ch *ChCommunityEpochs) FetchCommunityEpoch(inEpochID []byte, inLiveOnly bool) *pdi.CommunityEpoch {

    // TODO: Sort my time issued and do binary search
    for _, epoch := range ch.epochHistory {
        if bytes.Equal(epoch.EpochTID, inEpochID) {
            return epoch
        }
    } 

    return nil
}




// ChTalk -- ChAgent for ChProtocolTalk 
type ChTalk struct {
    ChStore

}

// MergePost -- see ChAgent.MergePost
func (ch *ChTalk) MergePost(
    entry *chEntry,
) error {

    return nil
}



// ChSpace -- ChAgent for ChProtocolTalk 
type ChSpace struct {
    ChStore

}

// MergePost -- see ChAgent.MergePost
func (ch *ChSpace) MergePost(
    entry *chEntry,
) error {

    return nil
}












// IsEmpty returns true if this IntSet contains no values.
func (set *IntSet) IsEmpty() bool {

    if len(set.Ints) == 0 && set.From >= set.To {
        return true
    }

    return false
}


