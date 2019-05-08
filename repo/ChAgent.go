package repo

import (
    "bytes"
    //"log"
    //"os"
    //"path"
    //"io/ioutil"
    //"strings"
    "sync"
    "sync/atomic"
    //"context"
    //"fmt"
    //"sort"
   //"reflect"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"
    "encoding/binary"
    

    //"github.com/plan-systems/go-plan/pcore"
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
    BuiltinProtocolPrefix    = "ch/_/"

    // ChProtocolACC is used for RootACChannelID and all other ACCs
    ChProtocolACC            = BuiltinProtocolPrefix + "acc"

    // ChProtocolMemberRegistry is used for plan.MemberRegistryChannelID
    ChProtocolMemberRegistry = BuiltinProtocolPrefix + "member-registry"

    // ChProtocolTalk is for conventional group chat sequential text streams
    ChProtocolTalk           = BuiltinProtocolPrefix + "talk"

    // ChProtocolLog is a log of status, error, or anomaly reports.
    ChProtocolLog            = BuiltinProtocolPrefix + "log"
)




// ChAgent is a protocol-specific implementation built to interact with a ChStore
type ChAgent interface {

    Store() *ChStore

    Startup() error

    CanAcceptEntry(
        inChEntry ChEntryInfo,
    ) (*pdi.ChannelEpoch, error)

    PostEntry(
        inChTxn *badger.Txn,
        ioEntry *DecryptedEntry,
    ) (entryErr error, dbErr error)

    // Initiates a reversal and revalidation cascade for each recursive-reversed entry.
    ReverseEntry(
        inURID []byte,
    ) error


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


var (
    chStateKey = []byte{chStatePrefix, 0x00}

)

const (

    // Ch DB forks
    chEntryDepEpochKey byte = iota
    chEntryDepAuthorKey
    chEntryDepACCKey
    chEntryInfoKey
    chEntryAssetKey
    chAgentAssetKey
    chStatePrefix

    //hEntryForkPos = pdi.URIDSz

    chEntryDepKeySz    = 1 + 8 + plan.ChannelIDSz
    chEntryInfoKeySz   = 1 + pdi.URIDSz
    chEntryAssetKeySz  = 1 + pdi.URIDSz
)




/*
ChStoreDb  
        |
        + chEntryInfoKey    + Entry URID 1                  => ChEntryInfo
        |                   + Entry URID 2                  => ChEntryInfo
        |                   ...
        |
        + chEntryAssetKey   + Entry URID 1                  => ChAgent-specific asset
        |                   + Entry URID 2                  => ChAgent-specific asset
        |                   ...
        |
        + chEntryEpochDepKey + EpochID A + Dep chID 123     => worst(ChDependency)
        |                    + EpochID A + Dep chID 456     => worst(ChDependency)
        |                    + EpochID B + Dep chID 789     => worst(ChDependency)
        |                    ...
        |
        + chEntryAuthorDepKey + memberID + epochNum + Dep chID 123   => worst(ChDependency)
        |                     + memberID + epochNum + Dep chID 123   => worst(ChDependency)
        |                     + memberID + epochNum + Dep chID 123   => worst(ChDependency)
        |                     ...
        |
        + chAgentAssetKey   + ChAgent key 0                 => ChAgent-specific asset
        |                   + ChAgent key 1                 => ChAgent-specific asset
        |                   ...
        | 
        + chStatePrefix     + ChState (0x00)                => ChStoreState
                            + Reserved (0x01)
                             ...

*/




func (chState *ChStoreState) FetchChannelEpoch(inEpochID uint64) *pdi.ChannelEpoch {

    for _, epoch := range chState.EpochHistory {
        if epoch.EpochID == inEpochID || inEpochID == 0 {
            return epoch
        }
    }

    return nil
}




// ChStore is the low-level channel entry manager for a channel.  
type ChStore struct {

    State                   ChStoreState

    // A Ch store needs to have each active channel sessions available so changes/notifications can be sent out.
    ChSessions              []*ChSession

    // ChEntry keyed by entry URID
    db                     *badger.DB

    validationMutex         sync.Mutex      

    //HomePath                 string


    //entryInbox              chan *entryIP


    //ChannelID               plan.ChannelID

}




/*
func (chSt *ChStore) WriteAssetToDb(
    inDbFork byte,
    inAssetID []byte,
) int {
    
    //var scrap [
    sz := 0
    inScrap[sz] = inDbFork
    sz++

    sz += copy(inScrap[sz:], inAssetID)

    return sz
}

*/

// Store -- see ChAgent.Startup()
func (chSt *ChStore) Store() *ChStore {
    return chSt
}



// Startup -- see ChAgent.Startup()
func (chSt *ChStore) Startup() error {
    return nil
}



// NewWrite -- see ChAgent.NewWrite
func (chSt *ChStore) NewWrite(
) *badger.Txn {

    dbTxn := chSt.db.NewTransaction(true)

    return dbTxn
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

    sz, err := inEntry.ChEntry.MarshalTo(scrap[:])
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


// CanAcceptEntry -- see ChAgent.CanAcceptEntry
func (chSt *ChStore) CanAcceptEntry(
    inChEntry ChEntryInfo,
) (*pdi.ChannelEpoch, error) {

    var chEpoch *pdi.ChannelEpoch

    for _, i := range chSt.State.EpochHistory {
        if i.EpochID == inChEntry.ChannelEpochID {
            chEpoch = i
            break
        }
    } 

    if chEpoch == nil {
        return nil, plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v not found", inChEntry.ChannelEpochID)
    }

    // TODO: check that entry can post in this epoch, etc.


    return chEpoch, nil
}


// MergeEntry -- see ChAgent.MergeEntry
func (chSt *ChStore)  MergeEntry(
    inEntry *DecryptedEntry,
) error {

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

const chEntryScrapSz = 128
const chDepScrapSz = 64

func (chSt *ChStore) WriteEntryRaw(
    dbTxn *badger.Txn,
    inEntry *DecryptedEntry,
) error {
    
    var (
        scrap [chEntryScrapSz]byte
        entryKey [chEntryInfoKeySz]byte
    )

    entryKey[0] = chEntryInfoKey
    copy(entryKey[1:], inEntry.URID)

    sz, err := inEntry.ChEntry.MarshalTo(scrap[:])
    if err != nil {
        panic("failed to marshal ChEntryInfo")
    }
    err = dbTxn.Set(entryKey[:], scrap[:sz])

    if err == nil {
        entryKey[0] = chEntryAssetKey
        if err == nil && len(inEntry.ChAgentAsset) > 0 {
            err = dbTxn.Set(entryKey[:], inEntry.ChAgentAsset)
        }
    }

    return err
}


// AddDependency adds the given dependency, storing a state such that subsequent entries posted to reverted to this channel
//    can cause dependent channels to commence relvalidation. 
func (chSt *ChStore) AddDependency(
    inDepType ChDependencyType,
    inChID plan.ChannelID,
    inChEntry *ChEntryInfo,
) error {

    var (
        depKey [chEntryDepKeySz]byte
        chDep ChDependency
    )

    dep := uint64(0)
    switch inDepType {

        case ChDependencyType_BY_AUTHOR:
            dep = uint64(inChEntry.AuthorMemberID) << 32 | uint64(inChEntry.AuthorMemberEpoch)
            depKey[0] = chEntryDepAuthorKey

        case ChDependencyType_BY_CHANNEL:
            dep = inChEntry.ChannelEpochID
            depKey[0] = chEntryDepEpochKey

        case ChDependencyType_BY_ACC:
            dep = inChEntry.ACCEpochID
            depKey[0] = chEntryDepACCKey
    }

    binary.BigEndian.PutUint64(depKey[1:], dep)
    binary.BigEndian.PutUint64(depKey[9:], uint64(inDepChID))

    chTxn := chSt.NewWrite()

    writeUpdate := false

    inDepTime := inChEntry.TimeAuthored

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
  
// MarkForRevalidation signals that all entries on or after the given time index must be revalidated.
func (chSt *ChStore) MarkForRevalidation(
    inValidationRev int64,
    inTime int64,
) {

    // TODO: use mutex 
    //chSt.validationMutex.Lock()
    atomic.StoreInt64(&chSt.State.ValidationRev, inValidationRev)
    if inTime < chSt.State.ValidatedUpto {
        chSt.State.ValidatedUpto = inTime
    }

    // TODO: flush to disk

}

// RevalidateDependencies queues revalidation for all possible dependencies for the given entry.
//
// Pre: inChEntry has been posted to this channel.
func (chMgr *ChMgr)  RevalidateDependencies(
    ch ChAgent,
    chEntry *ChEntryInfo,
) {

   var (
        searchKey [1 + 8 + 8]byte
        chDep ChDependency
        depTypes [3]byte
        scrap [1 + 8]byte
    )

    searchKey := inChEntry.SetupDepSearchKey(scrap)



    depTypes := []byte{
        chEntryDepAuthorKey,
        chEntryDepEpochKey,
        chEntryDepACCKey,
    }


    for depKey[0] = range depTypes {

        switch depKey[0] {

            case chEntryDepAuthorKey:
                dep = uint64(inChEntry.AuthorMemberID) << 32 | uint64(inChEntry.AuthorMemberEpoch)

            case chEntryDepEpochKey:
                dep = inChEntry.ChannelEpochID

            case chEntryDepACCKey:
                dep = inChEntry.ACCEpochID
        }

        binary.BigEndian.PutUint64(depKey[1:], dep)
   
    }

        switch inDepType {

        case ChDependencyType_BY_AUTHOR:
            dep = uint64(inChEntry.AuthorMemberID) << 32 | uint64(inChEntry.AuthorMemberEpoch)
            depKey[0] = chEntryDepAuthorKey

        case ChDependencyType_BY_CHANNEL:
            dep = inChEntry.ChannelEpochID
            depKey[0] = chEntryDepEpochKey

        case ChDependencyType_BY_ACC:
            dep = inChEntry.ACCEpochID
            depKey[0] = chEntryDepACCKey
    }

        /*




    searchFor := uint64(0)
    switch inChEntry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH:
            searchFor = inChEntry.ChannelEpochID

    }

    if searchFor == 0 {
        return
    }

    // Make a new iterator, search for a match.
    // If we find a match, queue the revalidation
    chTxn := ch.Store().db.NewTransaction(false)

 

    opts := badger.DefaultIteratorOptions
    itr = chTxn.NewIterator(opts)
        
    searchKey[0] = chEntryDepEpochKey
    binary.BigEndian.PutUint64(searchKey[1:], searchFor)
    //binary.BigEndian.PutUint64(depKey[9:], chSt.State.ChannelID)
    
    // Start at the dependency, iterate thru all channel dependencies
    for itr.Seek(searchKey[:9]); itr.Valid(); itr.Next() {
        item := itr.Item()
        //copy(URID[1:] item.Key())

        itemKey = itr.Key()
        if bytes.HasPrefix(itemKey, searchKey[:9]) {
            depChID := binary.BigEndian.GutUint64(itemKey[9:])
            
            err := item.Value(func(v []byte) error {
                return chDep.Unmarshal(v)
            })

            depCh, entryErr = chMgr.FetchChannel(depChID, nil)
            depCh.revalQueue <- revalMsg{
                revealAfter: chDep.DepTime,
            }
        } else {
            break
        }
    }

    chTxn.Discard()

    }


func Revalidate(
    chMgr *ChMgr,
    ch ChAgent,
) error {

    var (
        revalMsg revalMsg
        gotMsg bool
    )

    chSt := ChAgent.Store()

    /*
    // Go through all the entries in this channel from the validation bookmark 
    entryKey[0] = chEntryInfoKey

    for {
        t := chSt.State.ValidatedUpto
        for i := 1 + pdi.URIDTimestampSz; i > 0; i-- {
            entryKey[i] = byte(t)
            t >>= 8
        }

    copy(entryKey[1:], inEntry.URID)

    sz, err := inEntry.ChEntry.MarshalTo(scrap[:])
    if err != nil {
        panic("failed to marshal ChEntryInfo")
    }
    err = dbTxn.Set(entryKey[:], scrap[:sz])

    if err == nil {
        entryKey[0] = chEntryAssetKey
        if err == nil && len(inEntry.ChAgentAsset) > 0 {
            err = dbTxn.Set(entryKey[:], inEntry.ChAgentAsset)
        }
    }

    return err 
    */

    var (
        scrap [chEntryScrapSz]byte
        entryKey [chEntryInfoKeySz]byte
        URID [pdi.URIDSz]byte
        entryInfo ChEntryInfo
    )



    isIdle := false
    doReset := true

    for {
        if isIdle {
            revalMsg, gotMsg = <- chSt.revalQueue
        } else {
            select {
                //URIDFromInfo 
                case revalMsg, gotMsg = <- chSt.revalQueue:
                default:
                    // If no reval msg is here, keep trygin to validate deferred entries!
            }
        }

        // Merge the revalMsg with the current reval state
        if gotMsg {
            //
        }

        if doReset {
            chTxn := chSt.NewWrite()

            entryKey[0] = chEntryInfoKey
            copy(entryKey[1:], chSt.State.ValidatedUpto)

            itr = chTxn.NewIterator(opt)
            itr.Seek()            
        } else {
            itr.Next()
        }

        if itr.Valid() {
            item := itr.Item()
            //copy(URID[1:] item.Key())
            err := item.Value(func(v []byte) error {
                err := entryInfo.Unmarshal(v)
                return err
            })

            status := entryInfo.Status

            chMgr.ValidateEntry(ch, &entryInfo, false)

            if entryInfo.Status != status {

                // If an entry is no longer live (and once was), propgigate the entry revalidation cascade.
                if entryInfo.Status != ChEntryStatus_LIVE {

                    {} // TODO
                }

                // Flush the updated entry 
                n, err = entryInfo.Marshal(scrap[:])
                err = chTxn.Set(itemKey, scrap[:])

                err = chTxn.Commit()
                ///doReset = true?
            } else {
                chTxn.Discard()
            }
        }


        
    }
}

/*
func (chSt *ChStore) WriteEntry(
    dbTxn *badger.Txn,
    inEntry *DecryptedEntry,
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
   



// InstantiateChAgent instantiates a new ChAgent for the given channel protocol string.
func InstantiateChAgent(
    chID plan.ChannelID,
    chDb *badger.DB,
    chGenesisEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {


    var (
        ch ChAgent
        err error
        chState ChStoreState
    )

    // First load the latest channel state object from the chDB
    if err == nil {
        dbTxn := chDb.NewTransaction(false)

        var item *badger.Item

        item, err = dbTxn.Get(chStateKey)
        if err == nil {
            err = item.Value(func(val []byte) error {
                return chState.Unmarshal(val)
            })
            if err == nil {
                if chState.ChannelID != uint64(chID) {
                    err = plan.Errorf(nil, plan.FailedToLoadChannel, "channel ID in state does not match %v, got %v", chID, chState.ChannelID)
                }
            }
        } else if err == badger.ErrKeyNotFound {
               // err = plan.Errorf(nil, plan.ChannelNotFound, "channel %v does not yet exist", chID)
            err = nil
            chState.ChannelID = uint64(chID)

            if chGenesisEpoch != nil {
                chState.EpochHistory = []*pdi.ChannelEpoch{ 
                    chGenesisEpoch,
                }
            }
        }

        dbTxn.Discard()
    }

 
    if err == nil {
        latestEpoch := chState.FetchChannelEpoch(0)

        if latestEpoch != nil {
            chProtocol := latestEpoch.Protocol
            agentFactory := gChAgentRegistry[chProtocol]
        
            if agentFactory == nil {
                ch = &ChUnknown{}
                //return nil, plan.Errorf(nil, plan.ChAgentNotFound, "no ChAgent registered for protocol %v", chProtocol)
            } else {
                ch = agentFactory(chProtocol)
            }
        }

        //err = ch.Startup(chSt)

        {
            chSt := ch.Store()
            chSt.State = chState
            chSt.db = chDb

            err = ch.Startup()
        }
    }

    if err != nil {
        return nil, err
    }

    return ch, nil
}


func init() {
        
    gChAgentRegistry[ChProtocolACC] = func(inChProtocol string) ChAgent {
        return &ChACC{}
    }
    gChAgentRegistry[ChProtocolMemberRegistry] = func(inChProtocol string) ChAgent {
        return &ChMemberRegistry{}
    }
    gChAgentRegistry[ChProtocolTalk] = func(inChProtocol string) ChAgent {
        return &ChTalk{}
    }
}

// ChUnknown -- ChAgent for a protocol not recognized or not yey known.
type ChUnknown struct {
    ChStore

}



// PostEntry -- see ChAgent.MergeEntry
func (ch *ChUnknown) PostEntry(
    chTxn *badger.Txn,
    ioEntry *DecryptedEntry,
) (entryErr error, processErr error) {

    return nil, nil
}



// ChACC -- ChAgent for ChProtocolACC 
type ChACC struct {
    ChStore

}


const (
    codecChEntryWhitelistURID = ChProtocolACC + "/ChEntryWhitelistID"
)


// PostEntry -- see ChAgent.MergeEntry
func (acc *ChACC) PostEntry(
    chTxn *badger.Txn,
    ioEntry *DecryptedEntry,
) (entryErr error, processErr error) {

    return nil, nil
}


func (acc *ChACC) IsEntryAuthorized(
    inEntryInfo *ChEntryInfo,
) ([]byte, error) {

    //entryStatus := ChEntryStatus_AWAITING_VALIDATION

    epoch := acc.State.FetchChannelEpoch(inEntryInfo.ACCEpochID)
    if epoch == nil {
        return nil, plan.Errorf(nil, plan.ChannelEpochNotFound, "channel epoch %v in acc %v not found", inEntryInfo.ChannelEpochID, acc.State.ChannelID)
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



    return epoch.EntryURID, nil

}


const (
    MemberEpochCodec = ChProtocolMemberRegistry + "/MemberEpoch"
)

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
    epochLookup         map[uint64]*pdi.MemberEpoch

}





/*
func (ch *ChMemberRegistry) Startup() error {

    if ch.epochLookup == nil {
        ch.epochLookup = map[uint64]*pdi.MemberEpoch{}
    }



    return nil
}
*/



// PostEntry -- see ChAgent.ReverseEntry
func (ch *ChMemberRegistry) PostEntry(
    chTxn *badger.Txn,
    ioEntry *DecryptedEntry,
) (entryErr error, processErr error) {

    body := plan.Block{}
    entryErr = body.Unmarshal(ioEntry.Body)

    epochList := ioEntry.ChAgentAsset[:0]

    if entryErr != nil {
        entryErr = plan.Error(entryErr, plan.UnmarshalFailed, "entry body not plan.Block")
    } else {
        err := body.FindBlocksWithCodec(
            MemberEpochCodec, 0,
            func (epochBlock *plan.Block) error {
                epoch := &pdi.MemberEpoch{}
                err := epoch.Unmarshal(epochBlock.Content)
                if err != nil {
                    err = plan.Error(err, plan.UnmarshalFailed, "error unmarshalling ChMemberRegistry MemberEpoch")
                } else {
                    var epochKey [1 + 4 + 4 + pdi.URIDSz]byte
                    epochKey[0] = chAgentAssetKey
                    binary.BigEndian.PutUint32(epochKey[1:5], epoch.MemberID)
                    binary.BigEndian.PutUint32(epochKey[5:9], epoch.EpochNum)
                    //copy(epochKey[9:], ioEntry.URID)

                    epoch.EntryURID = ioEntry.URID
                    epochBuf, _ := epoch.Marshal()
                    err = chTxn.Set(epochKey[:], epochBuf)

                    // Maintain a list of all the epochs we're writing
                    epochList = append(epochList, epochKey[1:9]...)
                }

                return err
            },
        )
        if plan.IsError(err, plan.UnmarshalFailed) {
            entryErr = err
        } else {
            processErr = err 
        }
    }

    // This agent asset allows us to reverse this entry
    ioEntry.ChAgentAsset = epochList

    return entryErr, processErr
}



func (ch *ChMemberRegistry) ValidateAuthor(
    inEntry *ChEntryInfo,
) ([]byte, error) {
     
    //registry.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_AUTHOR)

    authorEpoch := ch.FetchMemberEpoch(
        inEntry.AuthorMemberID, 
        inEntry.AuthorMemberEpoch,
    )
    if authorEpoch != nil {
        return nil, plan.Error(nil, plan.MemberEpochNotFound, "member epoch not found")
    }

    // TODO: validate entry timestamp
    { }

    // TODO 

    if ! bytes.Equal(authorEpoch.PubSigningKey, inEntry.AuthorSig) {
        return nil, plan.Error(nil, plan.FailedToProcessPDIHeader, "author signature verification failed")
    }

    return authorEpoch.EntryURID, nil
}

// FetchMemberEpoch returns the requested MemberEpoch
func (ch *ChMemberRegistry) FetchMemberEpoch(
    inMemberID uint32,
    inMemberEpoch uint32,
) *pdi.MemberEpoch {

    lookupKey := (uint64(inMemberID) << 32) | uint64(inMemberEpoch)

    ch.lookupMutex.RLock()
    epoch := ch.epochLookup[lookupKey]
    ch.lookupMutex.RUnlock()

    // If we didn't find the epoch already loaded, look it up in the db
    if epoch == nil {
        dbTxn := ch.db.NewTransaction(false)

        var epochKey [1 + 4 + 4]byte
        epochKey[0] = chAgentAssetKey
        binary.BigEndian.PutUint32(epochKey[1:5], inMemberID)
        binary.BigEndian.PutUint32(epochKey[5:9], inMemberEpoch + 1)

        // Walk in the reverse dir so we start from the newest rev
        opts := badger.IteratorOptions{
            PrefetchValues: true,
            PrefetchSize: 10,
            Reverse: true,
            Prefix: epochKey[:9],
        }

        itr := dbTxn.NewIterator(opts)

        epoch = &pdi.MemberEpoch{}
        var err error

        // Walk thru all URIDs of the this memberID+epochID pair.
        for itr.Seek(epochKey[:]); itr.Valid(); itr.Next() {
            item := itr.Item()

            err = item.Value(func (val []byte) error {
                return epoch.Unmarshal(val)
            })
            if err == nil {
                if epoch.MemberID != inMemberID || epoch.EpochNum != inMemberEpoch {
                    err = plan.Error(nil, plan.AssertFailed, "member epoch info does not match lookup")
                } else {

                    ch.lookupMutex.Lock()
                    ch.epochLookup[lookupKey] = epoch
                    ch.lookupMutex.Unlock()
                    break
                }
            }
 
            if err != nil {
                // TODO: log
            }  
        }
    
        dbTxn.Discard()

        if err != nil {
            return nil
        }  
          
    }

    return epoch

}




// ChTalk -- ChAgent for ChProtocolTalk 
type ChTalk struct {
    ChStore

}


// PostEntry -- see ChAgent.MergeEntry
func (ch *ChTalk) PostEntry(
    chTxn *badger.Txn,
    ioEntry *DecryptedEntry,
) (entryErr error, processErr error) {

    return nil, nil
}









// IsEmpty returns true if this IntSet contains no values.
func (set *IntSet) IsEmpty() bool {

    if len(set.Ints) == 0 && set.From >= set.To {
        return true
    }

    return false
}


