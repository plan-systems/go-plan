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



// Marshaler used to generalize serialization
type Marshaler interface {
	Marshal() ([]byte, error)
    MarshalTo([]byte) (int, error)
    Size() int
}

// Unmarshaler used to generalize serialization
type Unmarshaler interface {
	Unmarshal([]byte) error
}


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

    Startup() error

    ValidateEntry(
        inEntryURID []byte,
        inEntry *ChEntryInfo,
        chMgr *ChMgr,
    ) (EntryStatus, error)

    CanAcceptEntry(
        inChEntry ChEntryInfo,
    ) (*pdi.ChannelEpoch, error)


    MergeEntry(
        inEntry *DecryptedEntry,
    ) error

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

    ChannelID               []byte

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



func (chSt *ChStore) Startup() error {

    return nil
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
    inEntryURID []byte,
    inEntry *ChEntryInfo,
    chMgr *ChMgr,
) (EntryStatus, error) {

    var (
        //err error
        //authorEpoch *pdi.MemberEpoch
    )

    // 1 -- Check community membership records
    registry, err := chMgr.FetchMemberRegistry()
    err = registry.ValidateAuthor(inEntry)
    if err != nil {
        return EntryStatus_DEFERRED, err
    }

    // 2 -- Check entry's parent channel governing epoch
    chEpoch, err := chSt.CanAcceptEntry(*inEntry)
    if err != nil {
        return EntryStatus_DEFERRED, nil
    }

    //ch.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_CHANNEL)

    // 3 -- Check parent ACC
    acc, err := chMgr.FetchACC(chEpoch.AccessChannelID)
    if err != nil {
        return EntryStatus_DEFERRED, nil
    }

    status, authErr := acc.IsEntryAuthorized(inEntryURID, inEntry)
    if authErr != nil {
        return EntryStatus_DEFERRED, nil
    }

    //acc.AddDependency(inEntry.Info.ChannelID, inEntry.TimeAuthored, BY_ACC)

    
    return status, nil
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
    inEntryAsset []byte,
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
            err = dbTxn.Set(entryKey[:], inEntryAsset)
        }
    }

    return err
}


func (chSt *ChStore) WriteEntry(
    dbTxn *badger.Txn,
    inEntry *DecryptedEntry,
    inEntryAsset1 Marshaler,
) error {

    var scrap [1024]byte

    sz, err := inEntryAsset1.MarshalTo(scrap[:])
    buf := scrap[:sz]
    if err != nil {
        buf, err = inEntryAsset1.Marshal()
    }

    return chSt.WriteEntryRaw(dbTxn, inEntry, buf)

}



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
    chID []byte,
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
        } else if err == badger.ErrKeyNotFound {
            chState.GenesisMode = true
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
            chSt := ch.(*ChStore)
            chSt.State = chState
            chSt.db = chDb
            chSt.ChannelID = append(chSt.ChannelID[:0], chID...)

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


// MergeEntry -- see ChAgent.MergeEntry
func (ch *ChACC) MergeEntry(
    inEntry *DecryptedEntry,
) error {

    return nil
}


func (ch *ChACC) IsEntryAuthorized(
    inEntryURID []byte,
    inEntry *ChEntryInfo,
) (EntryStatus, error) {

    entryStatus := EntryStatus_NEEDS_VALIDATION

    epoch := ch.State.FetchChannelEpoch(inEntry.ChannelEpochID)
    if epoch != nil {

        // Normal permissions lookup. boring: do last :\
        { }

        if epoch.Extensions != nil {
            epoch.Extensions.FindBlocksWithCodec(codecChEntryWhitelistURID, 0, func(inMatch *plan.Block) error {
                if bytes.Equal(inEntryURID, inMatch.Content) {
                    entryStatus = EntryStatus_LIVE
                }
                return nil
            })
        }
    }

    return entryStatus, nil

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






func (ch *ChMemberRegistry) Startup() error {

    if ch.epochLookup == nil {
        ch.epochLookup = map[uint64]*pdi.MemberEpoch{}
    }



    return nil
}



// MergeEntry -- see ChAgent.MergeEntry
func (ch *ChMemberRegistry) MergeEntry(
    ioEntry *DecryptedEntry,
) error {

 
    dbTxn := ch.db.NewTransaction(true)

    body := plan.Block{}
    err := body.Unmarshal(ioEntry.Body)

    var scrap [128]byte
    epochList := scrap[:0]

    ioEntry.Status = EntryStatus_MALFORMED

    if err != nil {
        ioEntry.Status = EntryStatus_MALFORMED
    } else {
        err = body.FindBlocksWithCodec(
            MemberEpochCodec, 0,
            func (match *plan.Block) error {
                epoch := &pdi.MemberEpoch{}
                err := epoch.Unmarshal(match.Content)
                if err != nil {
                    ioEntry.Status = EntryStatus_MALFORMED
                } else {
                    var epochKey [1 + 4 + 4 + pdi.URIDBinarySz]byte
                    epochKey[0] = chAgentForkPrefix
                    binary.BigEndian.PutUint32(epochKey[1:5], epoch.MemberID)
                    binary.BigEndian.PutUint32(epochKey[5:9], epoch.EpochNum)
                    copy(epochKey[9:], ioEntry.URID)
                    err = dbTxn.Set(epochKey[:], match.Content)

                    // Maintain a list of all the epochs we're writing
                    epochList = append(epochList, epochKey[1:9]...)
                }

                return err
            },
        )
    }

    if err == nil {
        err = ch.WriteEntryRaw(dbTxn, ioEntry, epochList)
    }

    if err == nil {
        err = dbTxn.Commit()
    } else {
        dbTxn.Discard()
    }



  
    return nil
}


func (ch *ChMemberRegistry) WriteGenesis(
    inChEntry ChEntryInfo,
    inBody []byte,
) error { 

    return nil
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

// MergeEntry -- see ChAgent.MergeEntry
func (ch *ChTalk) MergeEntry(
    inEntry *DecryptedEntry,
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
