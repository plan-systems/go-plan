package repo

import (
    "bytes"
    //"log"
    //"os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    //"sync/atomic"
    //"context"
    "fmt"
    //"sort"
   //"reflect"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"
    //"encoding/binary"
    

    "github.com/plan-systems/go-plan/pcore"
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





/*
Channel TODO:

/plan/ch/ch-registry ??


/plan/ch/talk
/plan/ch/space
/plan/ch/notepad
/plan/ch/sheet

/plan/ch/file/audio
/plan/ch/file/video


*/


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

    // ChProtocolWorkstationFleet allows a member to coordinate and allocate workstation IDs.
    ChProtocolWorkstationFleet      = BuiltinProtocolPrefix + "ws-fleet"

    // ChProtocolLog is a log of status, error, or anomaly reports.
    ChProtocolLog                   = BuiltinProtocolPrefix + "log"
)



/*
type ChAgentBase struct {
    chSt *ChStore
}

func (agent *ChAgentBase) AttachToChStore(chSt *ChStore) {
    agent.chSt = chSt
}

*/


// ChAgent is a protocol-specific implementation built to interact with a ChStore
type ChAgent interface {


    //Revalidator()

    //AttachToChStore(chSt *ChStore)
    Store() *ChStore

    Startup() error

    //NewAssetForContent() pcore.Marshaller

    OnLivenessChanged(
        entry *chEntry,
    ) error

    
/*
    CanAcceptEntry(
        inChEntry ChEntryInfo,
    ) (*pdi.ChannelEpoch, error)
*/

    MergePost(
        entry *chEntry,
    ) error


    MergeNewChannelEpoch(
        entry *chEntry,
    ) error

    // Initiates a reversal and revalidation cascade for each recursive-reversed entry.
    /*
    ReverseEntry(
        inURID []byte,
    ) error*/


/*
    ProcessNewEntry(
        inEntryBody []byte,
        inEntryURID []byte,
        ioEntryStatus *EntryStatus,
        outRetainBody *bool,
        outBody []byte,
    ) 

    EntryStatusChanged(
        inEntryURID []byte,
        ioEntryStatus *EntryStatus,
    )
*/

/*
    BeginSession(
        inCommunityRepo    *CommunityRepo,
        inMemberSession    *MemberSession,
        inChID             plan.ChannelID,
        inChMsgOutbox      chan *plan.Block,
    ) (ChSession, error)

    ProcessSession(sess *ChSession)
*/


    //OnEntryPosted()


    //OnEntryStatusChanged()
}


const (

    chEntryInfoKey byte = iota
    chEntryStateKey
    chEntryBodyKey
    chEntryDepsKey

    chEntryKeySz        = plan.TIDSz + 1
    chEntryTypePos      = plan.TIDSz
    chEntryDepKeySz     = chEntryKeySz + plan.ChIDSz
)


var (
    chStatePrefix   = []byte{0, 0, 0, 0, 0, 0, 0, 0}
    chBootstrapKey  = append(chStatePrefix, 0x00)
    chStateKey      = append(chStatePrefix, 0x01)

    chEntryStart    = []byte{0, 0, 1, 0, 0, 0, 0, 0}
)


/*
ChStore.db 
    |
    + chBootstrapKey                            => ChStoreBootstrap
    |
    + chStateKey                                => ChStoreState
    +
    + chAgentAssetKey + chAgentAssetID A        => entry.Body, custom, etc
    |                 + chAgentAssetID B        => entry.Body, custom, etc
    |                 ...
    |
    + chEntryStart
    |
    + EntryTID 1 + chEntryInfoKey               => entry.Info (ChEntryInfo)
    |            |
    |            + chEntryStateKey              => ChEntryState    
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









//func (guide *revalGuide) queueReval()



// ChStore is the low-level channel entry manager for a channel.  
type ChStore struct {

    State                   ChStoreState

    // A Ch store needs to have each active channel sessions available so changes/notifications can be sent out.
    ChSessions              []*ChSession

    // ChEntry keyed by entry URID
    db                     *badger.DB

    epochMutex              sync.RWMutex
    epochCache              map[plan.TIDBlob]*pdi.ChannelEpoch 
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


// Log is a convenience function to log errors.  
// This function has no effect if inErr == nil.
func (chSt *ChStore) Log(inErr error, inMsg string) {
    if inErr != nil {
        chSt.chMgr.flow.LogErr(inErr, inMsg)
    }
}

func (chSt *ChStore) Infof(inFormat string, inArgs ...interface{}) {
    suffix := fmt.Sprintf(inFormat, inArgs...)
    chSt.chMgr.flow.Log.Infof("ch %s (%s): %s\n", plan.ChID(chSt.State.ChannelID).String(), path.Base(chSt.State.ChProtocol), suffix) 
}

// Store -- see ChAgent.Startup()
func (chSt *ChStore) Store() *ChStore {
    return chSt
}

// ChannelID returns the channel ID for this ChStore
func (chSt *ChStore) ChannelID() plan.ChID {
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

    if inBlockUntilComplete {
        chSt.chShutdown.Wait()
    }
}

// FinishShutdown initiates a channel shutdown and blocks until complete.
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

/*
func loadEntry(
    inEntryID plan.TID,
    inTxn    *badger.Txn,
    loadBody func(v []byte) error,
    loadBodyRaw bool,
    entry *chEntry,
 ) *chEntry {

}
*/


// LoadEntry reads the given entry ID from this ChStore.
func (chSt *ChStore) loadEntry(
    inEntryID plan.TID,
    inTxn    *badger.Txn,
    outInfo  *ChEntryInfo, 
    outState *ChEntryState,
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

    // ChEntryInfo
    if outInfo != nil && err == nil {

        // Reset all fields
        outInfo.EntryOp = pdi.EntryOp(0)
        outInfo.SupersedesEntryID = outInfo.SupersedesEntryID[:0]
        outInfo.Extensions = nil
        outInfo.AuthorSig = outInfo.AuthorSig[:0]
        outInfo.AuthorMemberID = 0
        
        entryKey[chEntryTypePos] = chEntryInfoKey
        item, err = chTxn.Get(entryKey[:])
        if err == nil {
            err = item.Value(func (v []byte) error {
                return outInfo.Unmarshal(v)
            })
        }
        chSt.Log(err, "loading ChEntryInfo")
    }

    // ChEntryState
    if outState != nil && err == nil {

        outState.Flags = 0
        outState.Status = ChEntryStatus(0)
        outState.LiveIDs = outState.LiveIDs[:0]

        entryKey[chEntryTypePos] = chEntryStateKey
        item, err = chTxn.Get(entryKey[:])
        if err == nil {
            err = item.Value(func (v []byte) error {
                return outState.Unmarshal(v)
            })
        }
        chSt.Log(err, "loading ChEntryState")
    }

    // entry body
    if err == nil {
        if loadBody != nil {
            entryKey[chEntryTypePos] = chEntryBodyKey
            item, err = chTxn.Get(entryKey[:])
            if err == nil {
                err = item.Value(loadBody)
            }
            chSt.Log(err, "loading entry body")
        }
    }

    return err
}


func (chSt *ChStore) loadNextEntry(
    inTxn *badger.Txn,
    inPrevTID plan.TID,
    entry *chEntry,
    loadBody func(v []byte) error,
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

    entry.Reuse()
    err := chSt.loadEntry(
        entryKey[:], 
        chTxn,
        &entry.Info,
        &entry.State,
        loadBody,
    )
    if err != nil {
        panic(err)
    }

    entry.StatePrev = entry.State
    entry.flushState = false
    entry.flushBody = false

    return true, err

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
    if len(inPrevTID) == plan.TIDSz {
        copy(outEntryTID, inPrevTID)
        for j := plan.TIDSz - 1; j > 0; j-- {
            outEntryTID[j]++
            if outEntryTID[j] > 0 {
                break
            }
        }
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
        nil,
    )

    return haveEntry

}


/*


    // If we found another entry, validate it 
    {

        // Load ChEntryInfo
        item := itr.Item()
        key := item.Key()
        if len(key) == chEntryKeySz && key[chEntryTypePos] == chEntryInfoKey {
            chSt.Log(item.Value(func(v []byte) error {
                return entry.Info.Unmarshal(v)
            }), "loading ChEntryInfo")
        }

        // Load ChEntryState
        itr.Next()
        item = itr.Item()
        key = item.Key()
        if len(key) == chEntryKeySz && key[chEntryTypePos] == chEntryStateKey {
            chSt.Log(item.Value(func(v []byte) error {
                return entry.State.Unmarshal(v)
            }), "loading ChEntryState")
        }

        // If the entry is still awaiting merge, it means the entry asset stored is the body and we restore entry as if it's an incoming merge.
        entry.Body = entry.Body[:0]
        if entry.State.Status == ChEntryStatus_AWAITING_MERGE {
            var err error

            // Load body
            itr.Next()
            item = itr.Item()
            key = item.Key()
            if len(key) == chEntryKeySz && key[chEntryTypePos] == chEntryBodyKey {
                chSt.Log(item.Value(func(v []byte) error {
                    entry.Body = append(entry.Body, v...)
                }), "loading entry body")
            }
        }
    }
    */


func (chSt *ChStore) loadOriginalEntryBody(
    entry *chEntry,
) error {

    chTxn := chSt.db.NewTransaction(false)
    defer chTxn.Discard()

    err := chSt.loadEntry(
        entry.Info.EntryID(),
        chTxn,
        nil,
        nil,
        func(v []byte) error {
            entry.Body = append(entry.Body[:0], v...)
            return nil
        },
    )

    return err
}



// loadLatestEntryBody reads the given entry ID from this ChStore.
func (chSt *ChStore) loadLatestEntryBody(
    inTxn *badger.Txn,
    inEntryID plan.TID,
    outInfo *ChEntryInfo,
    outBody pcore.Marshaller,
) error {

  // If we didn't find the epoch already loaded, look it up in the db
    var (
        entryState ChEntryState
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
        if entryState.Status == ChEntryStatus_LIVE {
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
    v2 pcore.Marshaller,
) (int, error) {

    N := len(entry.scrap)
    if N - n < 1000 {
        entry.scrap = make([]byte, 2000)
        n = 0
    }

    if v2 != nil {
        sz, err := v2.MarshalTo(entry.scrap[n:])
        valBuf = entry.scrap[n:n+sz]
        if err == nil {
            n += sz
        } else {
            valBuf, err = v2.Marshal()
        }
    }

    if entryID == nil {
        entryID = entry.Info.EntryID()
    }

    keyPos := n
    n += copy(entry.scrap[n:], entryID)
    entry.scrap[n] = inKey
    n++

    err := wb.Set(entry.scrap[keyPos:n], valBuf, 0)
    
    return n, err
}



// flushEntry writes any dirty parts of chEntry to the channel db
func (chSt *ChStore) flushEntry(entry *chEntry) error {

    // Exit if there's no work to do
    if ! entry.flushState && ! entry.flushBody && len(entry.Info.SupersedesEntryID) == 0 {
        return nil
    }

    wb := chSt.db.NewWriteBatch()

    var (
        err error
        n int
    )

    entryID := entry.Info.EntryID()

    // If we're merging, write out info and body entries (they only need to be written once)
    if entry.flushBody {
        n, err = writeEntryItem(wb, entry, entryID, chEntryInfoKey, n, nil, &entry.Info)
        chSt.Log(err, "storing ChEntryInfo")
        
        if err == nil {
            n, err = writeEntryItem(wb, entry, entryID, chEntryBodyKey, n, entry.Body, nil)
            chSt.Log(err, "storing entry.Body")
        }
    }

    // If this entry is now live and supercedes another, update the superceded entry's state to include the superceded entry ID.
    // An entry never places it's own entry ID in it's ChEntryState live IDs since it is implied.
    if len(entry.Info.SupersedesEntryID) > 0 {
        if entry.LivenessChanged() {
            var supersededState ChEntryState
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
            chSt.Log(err, "storing superceded ChEntryState")
        }
    }

    if entry.flushState && err == nil {
        n, err = writeEntryItem(wb, entry, entryID, chEntryStateKey, n, nil, &entry.State)
        chSt.Log(err, "storing ChEntryState")
    }


    if err == nil {
        err = wb.Flush()
        chSt.Log(err, "flushing chEntry")
    } else {
        wb.Cancel()
    }

    if err == nil {
        entry.flushBody = false
        entry.flushState = false
    }

    return err
}




/*
func (chSt *ChStore) AddDependency(
    inDepChID []byte,
    inTime int64,
    inType ChDependencyType,
) error {


    dbTxn := chSt.db.NewTransaction(true)

    var (
        scrap [128]byte
        depKey [kChEntryDepKeySz]byte
    )

    copy(depKey[:], inEntry.URID)
    entryKey[kChEntryForkPos] = entryInfo

    sz, err := inentry.Info.MarshalTo(scrap[:])
    if err != nil {
        panic("failed to marshal ChEntryInfo")
    }
    err = dbTxn.Set(entryKey[:], scrap[:sz])

    if err == nil {
        entryKey[entryForkPos] = entryAsset
        if err == nil {
            err = dbTxn.Set(entryKey[:], inEntryAsset)
        }
    }


    if err == nil {
        err = ch.WriteEntryRaw(dbTxn, ioEntry, epochList)
    }

    if err == nil {
        err = dbTxn.Commit()
    } else {
        dbTxn.Discard()
    }
    */




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


/*
// OnLivenessChanged -- see ChAgent.OnLivenessChanged
func (chSt *ChStore) LoadBestEntryBody(
    inLiveAsset *ChLiveAsset,
    outBody pcore.Unmarshaller,
) error {

    chTxn := chSt.db.NewTransaction(false)

    if chTxn.Get(
    inLiveAsset
    return nil

}
*/


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
        var entryInfo ChEntryInfo

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



// CanAcceptEntry -- see ChAgent.CanAcceptEntry
func (chSt *ChStore) CanAcceptEntry(
    entryInfo *ChEntryInfo,
) (*pdi.ChannelEpoch, error) {

    epochID := entryInfo.ChannelEpochID()

    chEpoch := chSt.FetchChEpoch(epochID)

    if chEpoch == nil {
        return nil, plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v not found", epochID)
    }

    if len(entryInfo.SupersedesEntryID) > 0 && ! chEpoch.EntriesSupersedable {
        return nil, plan.Errorf(nil, plan.ChannelEpochDisallows, "channel epoch does not allow entries ot be superseded")
    }

    // TODO: check that entry can post in this epoch, etc.


    return chEpoch, nil
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

/*
func (chSt *ChStore) WriteEntryRaw(
    dbTxn *badger.Txn,
    entry *chEntry,
) error {
    
    var (
        scrap [chEntryScrapSz]byte
        entryKey [chEntryKeySz]byte
    )

    entryKey[0] = chEntryInfoKey
    copy(entryKey[1:], inEntry.URID)

    sz, err := inentry.Info.MarshalTo(scrap[:])
    entryBuf := scrap[:sz]
    if err != nil {
        entryBuf, err = inentry.Info.Marshal()
    }
    chSt.Log(err, "ChEntry marshal failed")

    err = dbTxn.Set(entryKey[:], entryBuf)
    chSt.Log(err, "store ChEntry error")

    
    if err == nil {
        entryKey[0] = chEntryAssetKey
        if err == nil && len(inEntry.ChAgentAsset) > 0 {
            err = dbTxn.Set(entryKey[:], inEntry.ChAgentAsset)
        }
    }

    return err
}
*/


// ValidateEntry checks that the conditions needed for an entry to go live. 
func (chSt *ChStore) ValidateEntry(
    entry *chEntry,
) (entryErr error, processErr error) {


    var (
        accID plan.ChID
        //authURID []byte
        //err error
        //authorEpoch *pdi.MemberEpoch
        writeDeps bool
        depsWritten int
    )

    if ! entry.HasFlags(ChEntryFlag_DEPENDENCIES_WRITTEN) {
        writeDeps = true
    }

    status := ChEntryStatus_LIVE

    if ! entry.HasFlags(ChEntryFlag_GENESIS_ENTRY_VERIFIED, ChEntryFlag_WELL_FORMED) {
    
        // 1 -- Check community membership records
        {
            registry, err := chSt.chMgr.FetchMemberRegistry()
            if err != nil {
                processErr = err
            } else {
                err = registry.ValidateAuthor(&entry.Info)

                // TODO: handle when member registry returns an err
                if writeDeps {
                    processErr = registry.AddDependency(chSt.ChannelID(), &entry.Info)
                    if processErr == nil {
                        depsWritten++
                    }
                }
            }
            if err != nil {
                status = ChEntryStatus_DEFERRED
            }
        }

        // 2 -- Check entry's parent channel governing epoch
        {
            chEpoch, err := chSt.CanAcceptEntry(&entry.Info)
            if err != nil {
                processErr = err

                status = ChEntryStatus_DEFERRED

            } else {

                // Note: no dependency is created here b/c if/when a channel entry is overturned, subsequent entries in that channel always undergo revalidation,
                accID = chEpoch.ACC
            }
        }

        // 3 -- Check parent ACC
        {
            acc, err := chSt.chMgr.FetchACC(accID)
            if err == nil {
                err = acc.IsEntryAuthorized(entry)
                if writeDeps {
                    processErr = acc.AddDependency(chSt.ChannelID(), &entry.Info)
                    if processErr == nil {
                        depsWritten++
                    }
                }
            } 
            
            if err != nil {
                status = ChEntryStatus_DEFERRED
            }
        }
    }

    if depsWritten == 2 {
        entry.AddFlags(ChEntryFlag_DEPENDENCIES_WRITTEN)
    }

    entry.SetStatus(status)

    return entryErr, processErr
}




/*
func formDepKeyForAuthor(
    authorMemberID uint32,
    authorEpochNum uint32,
) uint64 {
    return uint64(authorMemberID) << 32 | uint64(authorEpochNum)
}
*/

// AddDependency adds the given dependency, storing a state such that subsequent entries posted to reverted to this channel
//    can cause dependent channels to commence relvalidation. 
func (chSt *ChStore) AddDependency(
    inChID plan.ChID,
    inChEntry *ChEntryInfo,
) error {

    var (
        depKey [chEntryDepKeySz]byte
        chDep ChDependency
    )

    plan.Assert(len(inChID) == plan.ChIDSz, "bad channel ID")

    copy(depKey[:], inChEntry.EntryID())
    depKey[plan.TIDSz] = chEntryDepsKey
    copy(depKey[chEntryKeySz:], inChID)

    // Typically, we won't need to write but we have to do it anyway in the event that we do.
    chTxn := chSt.NewWrite()
    needsUpdate := false

    depTime := int64(inChEntry.TimeAuthoredFS())

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

/*

func (chSt *ChStore) AddDependency(
    inAuthURID []byte,
    inDepChID uint64,
    inDepTime int64,
) error {


    var (
        depKey [chEntryDepKeySz]byte
        chDep ChDependency
    )

    depKey[0] = chEntryDepKey
    copy(depKey[1:], inAuthURID)

    binary.BigEndian.PutUint64(depKey[1+pdi.URIDSz:], inDepChID)

    chTxn := chSt.NewWrite()

    writeUpdate := false

    // If there's already a dependency for the referenced URID in the given channel, 
    //    choose the more restrictive dependency.
    item, err := chTxn.Get(depKey[:])
    if err == nil {
        err = item.Value(func(val []byte) error {
            return chDep.Unmarshal(val)
        })
        if err == nil {
            if inDepTime < chDep.DepTime {
                writeUpdate = true
            }
        }
    } else if err == badger.ErrKeyNotFound {
        err = nil
        writeUpdate := true
    }

    if err == nil && writeUpdate {
        chDep.DepTime = inDepTime
        var scrap [chDepScrapSz]byte
        len, err := chDep.MarshalTo(scrap[:])
        if err != nil {
            panic(err)
        }

        err = chTxn.Set(depKey[:], scrap[:])
        if err != nil {
            panic(err)
        }

        // TODO: handle failed commit.  When exactly can commits fail??
        err = chTxn.Commit()
        if err != nil {
            panic(err)
        }

    } else {
        chTxn.Discard()
    }

    return err
}
*/
  

// RevalidateDependencies queues revalidation for all possible dependencies for the given entry.
//
// Pre: inChEntry has been posted to this channel.
func (chSt *ChStore) RevalidateDependencies(
    chEntry *ChEntryInfo,
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
                chSt.chMgr.flow.LogErr(err, "error loading ch dep")
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

    chSt.Infof("QueueRevalidation to %v", inAfterTime)

    chSt.revalRequestMutex.Lock()
    if inAfterTime < chSt.revalAfter {
        chSt.revalAfter = inAfterTime
        chSt.Infof("revalAfter = %v", inAfterTime)
    }
    chSt.revalRequestMutex.Unlock()
}

// dequeueRevalidation flushes queued revalidation messages with the current reval state.
func (chSt *ChStore) dequeueRevalidation() {

    if len(chSt.State.ValidatedUpto) != plan.TIDSz {
        chSt.State.ValidatedUpto = make([]byte, plan.TIDSz)
    }

    chSt.revalRequestMutex.Lock()
    if plan.TID(chSt.State.ValidatedUpto).SelectEarlier(chSt.revalAfter) {
        chSt.Infof("rewinding re-validation head to %v", chSt.revalAfter)
    }
    chSt.revalAfter = plan.TimeFSMax
    chSt.revalRequestMutex.Unlock()

    // TODO: flush to disk so a crash doesn't cause validation state to get dropped

}


var entryPool = sync.Pool{
	New: func() interface{} {
		return new(chEntry)
	},
}

func chEntryAlloc() *chEntry {

    entry := entryPool.Get().(*chEntry)
    entry.Reuse()

    return entry
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
            entry.SetStatus(ChEntryStatus_MERGED)
        }

        // TODO: how do we handle malformed entries from here?
        { }
    }

    return err
}


func startupChannel(ch ChAgent) error {

    err := ch.Startup()

    if err == nil {
        chSt := ch.Store()

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
       // revalMsg revalMsg
        shuttingDown bool
        entryTmp *chEntry
    )

    moreToValidate := true

    //opts := badger.DefaultIteratorOptions
    //opts.PrefetchValues = false

    chSt.processingEntries = true
    chSt.chShutdown.Add(1)

    for {

        var (
            entry *chEntry
        )

        // Prioritize entries that are ready to merge over entries pending valiation
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
                entryPool.Put(entryTmp)
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
        
        {
            updateValidationBookmark := false

            // Do initial merge or load the ChEntry from the chDB
            if entry != nil {
                // No op
            } else if ! chSt.State.MergeEnabled {
                moreToValidate = false
            } else {
                if entryTmp == nil {
                    entryTmp = chEntryAlloc()
                }

                chSt.Infof("%s", "loadNextEntryToValidate") 

                if chSt.loadNextEntryToValidate(entryTmp) {
                    entry = entryTmp
                    updateValidationBookmark = true
                    moreToValidate = true

                    // If we still haven't merged yet, we need to load the body, as if it just arrived chSt.entriesToMerge.
                    // Since this only occurs when chSt.State.MergeEnabled == false later turns true, this is rare and does not have to be efficient
                    if entry.State.Status == ChEntryStatus_AWAITING_MERGE {
                        chSt.Infof("%s", "loadOriginalEntryBody")
                        chSt.loadOriginalEntryBody(entry)
                    }
                } else {
                    chSt.Infof("%s", "moreToValidate = false") 
                    moreToValidate = false
                }
            }

            if entry == nil {
                continue
            }
            
            var err error
 
            chSt.Infof("processing entry %v (op: %v)", entry.Info.EntryID().String(), pdi.EntryOp_name[int32(entry.Info.EntryOp)])

            // Does the entry need to be merged?
            if entry.State.Status == ChEntryStatus_AWAITING_MERGE {
                chSt.Infof("%s", "merging entry")
                err = mergeEntry(ch, entry)
            }

            switch entry.State.Status {
                case ChEntryStatus_MERGED:      fallthrough 
                case ChEntryStatus_DEFERRED:    fallthrough 
                case ChEntryStatus_LIVE:
                    chSt.Infof("%s", "validating entry")
                    chSt.ValidateEntry(entry)
            }

            // flush any changes to the entry to the channel db
            err = chSt.flushEntry(entry)

            if err != nil {
                err = chSt.chMgr.flow.FilterFault(err)
            } else {

                if updateValidationBookmark {
                    copy(chSt.State.ValidatedUpto, entry.Info.EntryID())
                }

                // If the status of this entry has changed, revalidate channels that are known to be dependent.
                if entry.LivenessChanged() {
                    chSt.RevalidateDependencies(&entry.Info)

                    ch.OnLivenessChanged(entry)
                }
            }
        }

    }

    chSt.chShutdown.Done()

}

/*
func (chSt *ChStore) WriteEntry(
    dbTxn *badger.Txn,
    entry *chEntry,
    inEntryAsset pcore.Marshaller,
) error {

    var scrap [1024]byte

    sz, err := inEntryAsset.MarshalTo(scrap[:])
    buf := scrap[:sz]
    if err != nil {
        buf, err = inEntryAsset.Marshal()
    }

    return chSt.WriteEntryRaw(dbTxn, inEntry, buf)

}*/



/*
    dbTxn := chSt.db.NewTransaction(true)

    // Setup the entry key
    var entryKey chEntryKey 
    entryKey[0] = chDbForkEntries
    copy(entryKey[1:], inURID)

    sz, err := inChEntry.MarshalTo(scrap[:])

    if err == nil {
        entryKey[entryForkPos] = entryForkChEntryInfo
        err = dbTxn.Set(entryKey[:], scrap[:sz])
    }

    if err == nil {
        entryKey[entryForkPos] = entryForkChEntryBody
        err = dbTxn.Set(entryKey[:], inEntryBody)
    }

    if err == nil {
        err = dbTxn.Commit()
    } else {
        dbTxn.Discard()
    }*/
   

func (state *ChEntryState) getLiveIndex(
    inID plan.TID,
) int {

    const sz = plan.TIDSz
    N := len(state.LiveIDs)

    idx := 0

    if len(inID) != sz {
        for i := 0; i < N; {
            if bytes.Equal(inID, state.LiveIDs[i:i+sz]) {
                return idx 
            }
            idx++
            i += sz
        }
    }

    return -1
}


func (state *ChEntryState) AddLiveID(inID plan.TID) bool {

    if state.getLiveIndex(inID) < 0 {
        return false
    }

    state.LiveIDs = append(state.LiveIDs, inID...)  
    return true
}


func (state *ChEntryState) StrikeLiveID(inID plan.TID) bool {

    changed := false

    const sz = plan.TIDSz
    N := len(state.LiveIDs)

    for i := 0; i < N; {
        if bytes.Equal(inID, state.LiveIDs[i:i+sz]) {
            changed = true
            copy(state.LiveIDs[i:], state.LiveIDs[i+sz:])
            N -= sz
            state.LiveIDs = state.LiveIDs[:N]
        } else {
            i += sz
        }
    }

    return changed
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

    if inStatus == ChEntryStatus_LIVE {

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


const (
    codecChEntryWhitelistURID = ChProtocolACC + "/ChEntryWhitelistID"
)


// MergePost -- see ChAgent.MergePost
func (acc *ChACC) MergePost(
    entry *chEntry,
) error {

    return nil
}


func (acc *ChACC) IsEntryAuthorized(
    entry *chEntry,
) error {

    //entryStatus := ChEntryStatus_AWAITING_VALIDATION

    epoch := acc.FetchChEpoch(entry.Info.ACCEntryID())
    if epoch == nil {
        return plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v in acc %v not found", entry.Info.ChannelEpochID, acc.State.ChannelID)
    }

    // Normal permissions lookup. boring: do last :\
    { }

    /*
    if epoch.Extensions != nil {
        epoch.Extensions.FindBlocksWithCodec(codecChEntryWhitelistURID, 0, func(inMatch *plan.Block) error {
            if bytes.Equal(inEntryURID, inMatch.Content) {
                entryStatus = ChEntryStatus_LIVE
            }
            return nil
        })
    }*/



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

Db keys:
/URID/0: ChEntryInfo
/URID/1: IntSet (list of memberID epochs set by this entry)  
/AgentFork/memberID/epochID/URID: pdi.MemberEpoch
*/
type ChMemberRegistry struct {
    ChStore

    lookupMutex         sync.RWMutex
    epochLookup         map[plan.TIDBlob]*pdi.MemberEpoch

}





/*
func (ch *ChMemberRegistry) Startup() error {

    if ch.epochLookup == nil {
        ch.epochLookup = map[uint64]*pdi.MemberEpoch{}
    }



    return nil
}



func (ch *ChMemberRegistry) PrepapreToMerge(entry *chEntry) error {

    entry.
}


func (ch *ChMemberRegistry) BuildEntryAsset(entry *chEntry) error {

    memberEpoch := &pdi.MemberEpoch{}
    err := memberEpoch.Unmarshal(entry.Body)

    memberEpoch.EpochTID = entry.Info.EntryID()

    entry.Asset = memberEpoch
}
*/

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
    entryInfo *ChEntryInfo,
) error {
     
    //registry.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_AUTHOR)

    memberEpoch := ch.FetchMemberEpoch(entryInfo.AuthorEntryID())

    if memberEpoch != nil {
        return plan.Error(nil, plan.MemberEpochNotFound, "member epoch not found")
    }

    // TODO: validate entry timestamp
    { }


    if entryInfo.AuthorMemberID != memberEpoch.MemberID {
        return plan.Error(nil, plan.FailedToProcessPDIHeader, "author member ID verification failed")
    }
    if ! bytes.Equal(memberEpoch.PubSigningKey, entryInfo.AuthorSig) {
        return plan.Error(nil, plan.FailedToProcessPDIHeader, "author signature verification failed")
    }

    return nil
}

// FetchMemberEpoch returns the MemberEpoch associated with the given entry ID
func (ch *ChMemberRegistry) FetchMemberEpoch(inEntryID plan.TID) *pdi.MemberEpoch {

    entryID := inEntryID.Blob()

    // Is the epoch already loaded?
    ch.lookupMutex.RLock()
    epoch := ch.epochLookup[entryID]
    ch.lookupMutex.RUnlock()

    if epoch == nil {
        epoch = &pdi.MemberEpoch{}

        err := ch.loadLatestEntryBody(nil, inEntryID, nil, epoch)

        if err == nil {

            // Alternatively, we could write this when the entry is merged, but why waste space
            epoch.EpochTID = append(epoch.EpochTID[:0], inEntryID...)

            ch.lookupMutex.Lock()
            ch.epochLookup[entryID] = epoch
            ch.lookupMutex.Unlock()
        } else {
            epoch = nil
        }
    }

    return epoch

}




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

        commEpoch := &pdi.CommunityEpoch{}

        // TODO: handle error
        haveEntry, loadErr := ch.loadNextEntry(
            chTxn,
            entry.Info.EntryID(),
            &entry,
            func(v []byte) error {
                return commEpoch.Unmarshal(v)
            },
        )

        if ! haveEntry {
            break
        }

        if loadErr == nil {
            ch.epochHistory = append(ch.epochHistory, commEpoch)
        }
    }

    return err
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
    entry.flushBody = true
    
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



func (ch *ChCommunityEpochs) FetchCommunityEpoch(inEpochID []byte) *pdi.CommunityEpoch {

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









// IsEmpty returns true if this IntSet contains no values.
func (set *IntSet) IsEmpty() bool {

    if len(set.Ints) == 0 && set.From >= set.To {
        return true
    }

    return false
}


