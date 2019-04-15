package repo

import (
    "bytes"
    //"log"
    //"os"
    //"path"
    //"io/ioutil"
    //"strings"
    "sync"
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

    // ChProtocolACC is used for RootACChannelID and all other ACCs
    ChProtocolACC            = "/plan/ch/acc"

    // ChProtocolMemberRegistry is used for plan.MemberRegistryChannelID
    ChProtocolMemberRegistry = "/plan/ch/member-registry"

    // ChProtocolTalk is for conventional group chat sequential text streams
    ChProtocolTalk           = "/plan/ch/talk"

)




// ChAgent is a protocol-specific implementation built on ChStore
type ChAgent interface {

    Store() *ChStore

    Startup() error

    ValidateEntry(
        inEntryURID []byte,
        inEntry *ChEntryInfo,
        chMgr *ChMgr,
    )

    CanAcceptEntry(
        inChEntry ChEntryInfo,
    ) (*pdi.ChannelEpoch, error)

    PostNewEntry(
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
    chStateKey = []byte{chStateForkPrefix, 0x00}

)

const (

    // Ch DB forks
    chAgentForkPrefix = byte(0xFE)
    chStateForkPrefix = byte(0xFF)

    // chDbForkEntries sub forks
    chEntryInfo byte = iota
    chEntryAsset
    chEntryDeps

    chEntryForkPos = pdi.URIDBinarySz

    chEntryKeySz       = pdi.URIDBinarySz + 1
    chEntryAssetKeySz  = pdi.URIDBinarySz + 1
    chEntryDepKeySz    = pdi.URIDBinarySz + 1 + plan.ChannelIDSz
)




/*
ChStoreDb  
        |
        + Entry URID 1 + chEntryInfo  (0x00)               => ChEntryInfo
        |              + chEntryAsset (0x01)               => ChAgent-specific asset
        |              + chEntryDeps  (0x02) + dep chID 1  => min(ChDependency)
        |                                    + dep chID 2  => min(ChDependency)
        |                                    ...
        + ...
        |
        + Entry URID N + ...
        |
        |
        + chAgentForkPrefix (0xFE) + ChAgent key0 [+ URID]  => ChAgent-specific asset
        |                          |
        |                          + ChAgent key1 [+ URID]  => ChAgent-specific asset
        |                          |
        |                        ...
        | 
        + chStateForkPrefix (0xFF) + ChState (0x00)         => ChStoreState
                                   |
                                   + Reserved (0x01)
                                   |
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

    //HomePath                 string

    

    entryInbox              chan *entryIP


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



// StartMerge -- see ChAgent.StartMerge
func (chSt *ChStore) StartMerge(
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



func (chSt *ChStore) ValidateEntry(
    inChEntryURID []byte,
    inChEntry *ChEntryInfo,
    chMgr *ChMgr,
) {

    var (
        accID uint64
        //err error
        //authorEpoch *pdi.MemberEpoch
    )

    // 1 -- Check community membership records
    if inChEntry.MissingFlag(ChEntryFlag_AUTHOR_VALIDATED) {
        registry, err := chMgr.FetchMemberRegistry()
        err = registry.ValidateAuthor(inChEntry)
        if err == nil {
            inChEntry.AddFlags(ChEntryFlag_AUTHOR_VALIDATED)
        }
    }

    // 2 -- Check entry's parent channel governing epoch
    if inChEntry.MissingFlag(ChEntryFlag_CHANNEL_EPOCH_VALIDATED) || inChEntry.MissingFlag(ChEntryFlag_ACC_VALIDATED) {
        chEpoch, err := chSt.CanAcceptEntry(*inChEntry)
        if err == nil {
            inChEntry.AddFlags(ChEntryFlag_CHANNEL_EPOCH_VALIDATED)
            accID = chEpoch.AccessChannelID
        } else {
            inChEntry.ClearFlag(ChEntryFlag_CHANNEL_EPOCH_VALIDATED)
        }
    }

    //ch.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_CHANNEL)

    // 3 -- Check parent ACC
    if accID != 0  && inChEntry.MissingFlag(ChEntryFlag_ACC_VALIDATED) {
        acc, err := chMgr.FetchACC(accID)
        if err == nil {
            authErr := acc.IsEntryAuthorized(inChEntryURID, inChEntry)
            if authErr != nil {
                inChEntry.AddFlags(ChEntryFlag_ACC_VALIDATED)
            }
        }
    }

    //acc.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_ACC)
}



// ReverseEntry -- see ChAgent.CanAcceptEntry
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





 type YourT1 struct {}
  func (y YourT1) MethodBar() {
     //do something
  }

  type YourT2 struct {}
  func (y YourT2) MethodFoo(i int, oo string) {
     //do something
  }

  func Invoke(any interface{}, name string, args... interface{}) {
      inputs := make([]reflect.Value, len(args))
      for i, _ := range args {
          inputs[i] = reflect.ValueOf(args[i])
      }
      reflect.ValueOf(any).MethodByName(name).Call(inputs)
  }

 func main() {
      Invoke(YourT2{}, "MethodFoo", 10, "abc")
      Invoke(YourT1{}, "MethodBar")
 }

 func main() {
      Invoke(YourT2{}, "MethodFoo", 10, "abc")
      Invoke(YourT1{}, "MethodBar")
      */


func (chSt *ChStore) WriteEntryRaw(
    dbTxn *badger.Txn,
    inEntry *DecryptedEntry,
) error {
    
    var (
        scrap [128]byte
        entryKey [chEntryKeySz]byte
    )

    copy(entryKey[:], inEntry.URID)
    entryKey[chEntryForkPos] = chEntryInfo

    sz, err := inEntry.ChEntry.MarshalTo(scrap[:])
    if err != nil {
        panic("failed to marshal ChEntryInfo")
    }
    err = dbTxn.Set(entryKey[:], scrap[:sz])

    if err == nil {
        entryKey[chEntryForkPos] = chEntryAsset
        if err == nil {
            err = dbTxn.Set(entryKey[:], inEntry.ChAgentAsset)
        }
    }

    return err
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
            chState.ChannelID = uint64(chID)
            chState.EpochHistory = []*pdi.ChannelEpoch{ 
                chGenesisEpoch,
            }
            err = nil
        }

        dbTxn.Discard()
    }

 
    if err == nil {
        latestEpoch := chState.FetchChannelEpoch(0)

        chProtocol := latestEpoch.Protocol
        agentFactory := gChAgentRegistry[chProtocol]
        if agentFactory == nil {
            return nil, plan.Errorf(nil, plan.ChAgentNotFound, "no ChAgent registered for protocol %v", chProtocol)
        }

        ch = agentFactory(chProtocol)
        if err == nil {
            chSt := ch.Store()
            chSt.State = chState
            chSt.db = chDb

            err = ch.Startup()
        }
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




// ChACC -- ChAgent for ChProtocolACC 
type ChACC struct {
    ChStore

}


const (
    codecChEntryWhitelistURID = ChProtocolACC + "/ChEntryWhitelistID"
)


// PostNewEntry -- see ChAgent.MergeEntry
func (ch *ChACC) PostNewEntry(
    chTxn *badger.Txn,
    ioEntry *DecryptedEntry,
) (entryErr error, processErr error) {

    return nil, nil
}


func (ch *ChACC) IsEntryAuthorized(
    inEntryURID []byte,
    inEntry *ChEntryInfo,
) error {

    //entryStatus := ChEntryStatus_AWAITING_VALIDATION

    epoch := ch.State.FetchChannelEpoch(inEntry.ChannelEpochID)
    if epoch != nil {

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
    }

    return nil

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



// PostNewEntry -- see ChAgent.MergeEntry
func (ch *ChMemberRegistry) PostNewEntry(
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
            func (match *plan.Block) error {
                epoch := &pdi.MemberEpoch{}
                err := epoch.Unmarshal(match.Content)
                if err != nil {
                    err = plan.Error(err, plan.UnmarshalFailed, "error unmarshalling ChMemberRegistry MemberEpoch")
                } else {
                    var epochKey [1 + 4 + 4 + pdi.URIDBinarySz]byte
                    epochKey[0] = chAgentForkPrefix
                    binary.BigEndian.PutUint32(epochKey[1:5], epoch.MemberID)
                    binary.BigEndian.PutUint32(epochKey[5:9], epoch.EpochNum)
                    copy(epochKey[9:], ioEntry.URID)
                    err = chTxn.Set(epochKey[:], match.Content)

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

    ioEntry.ChAgentAsset = epochList

    return entryErr, processErr
}



func (ch *ChMemberRegistry) ValidateAuthor(
    inEntry *ChEntryInfo,
) error {
     
    //registry.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_AUTHOR)

    authorEpoch := ch.FetchMemberEpoch(
        inEntry.AuthorMemberID, 
        inEntry.AuthorMemberEpoch,
    )
    if authorEpoch != nil {
        return plan.Error(nil, plan.MemberEpochNotFound, "member epoch not found")
    }

    // TODO: validate entry timestamp
    { }

    // TODO 

    if ! bytes.Equal(authorEpoch.PubSigningKey, inEntry.AuthorSig) {
        return plan.Error(nil, plan.FailedToProcessPDIHeader, "PDI entry signature verification failed")
    }

    return nil
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

        var epochKey [1 + 4 + 4 + pdi.URIDBinarySz]byte
        epochKey[0] = chAgentForkPrefix
        binary.BigEndian.PutUint32(epochKey[1:5], inMemberID)
        binary.BigEndian.PutUint32(epochKey[5:9], inMemberEpoch + 1)

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


// PostNewEntry -- see ChAgent.MergeEntry
func (ch *ChTalk) PostNewEntry(
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
