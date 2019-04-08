package repo


import (
    //"bytes"
    "log"
    //"os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    "context"
    //"fmt"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    "encoding/base64"
    //"encoding/binary"

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

// ChMgr is the top level interface for a community's channels.
type ChMgr struct {
    flow                    plan.Flow

    HomePath                string

    //db                      *badger.DB
    //State                   ChMgrState

    fsNameEncoding          *base64.Encoding


    lookupMutex             sync.RWMutex
    hotChannels             map[plan.ChannelID]ChAgent

    chSessionsActive        sync.WaitGroup

}





var (
    //hMgrStateKey   = []byte{0}
    zero64          = []byte{0, 0, 0, 0, 0, 0, 0, 0}
)

func NewChMgr(
    inBaseDir string,
) *ChMgr {

    chMgr := &ChMgr{
        hotChannels: make(map[plan.ChannelID]ChAgent),
        fsNameEncoding: plan.Base64,
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

    // TODO me
    {}

    chMgr.chSessionsActive.Wait()

}


func (chMgr *ChMgr) InscribeGenesis(inSeed *RepoSeed) error {

    _, err := chMgr.FetchChannel(plan.RootACChannelID[:], inSeed.GenesisAccEpoch)
    if err != nil {
        return err
    }

    _, err = chMgr.FetchChannel(plan.MemberRegistryChannelID[:], inSeed.GenesisRegistryEpoch)
    if err != nil {
        return err
    }

    //chSt.MergeEntry()


    return nil
}




/*
func (chMgr *ChMgr) AuthenticateEntry(
    inEntry *pdi.EntryInfo,
    inSignerKey []byte,
) error {





}*/




// MergeEntry is called when an entry arrives to a repo and must be merged into the repo.
func (chMgr *ChMgr) MergeEntry(
    ioEntry *DecryptedEntry,
) {

    var (
        ch ChAgent
        err error
        chGenesis *pdi.ChannelEpoch
    )


    switch ioEntry.Info.EntryOp {
        
        case pdi.EntryOp_NEW_CHANNEL_EPOCH: {
            chGenesis = &pdi.ChannelEpoch{}
            err := chGenesis.Unmarshal(ioEntry.Body)
            if err != nil {
                ioEntry.Status = EntryStatus_MALFORMED
                err = plan.Error(err, plan.ChEntryIsMalformed, "error unmarshalling EntryOp_NEW_CHANNEL_EPOCH")
            }
        }
    }

    if err == nil {
        ch, err = chMgr.FetchChannel(ioEntry.Info.ChannelID, chGenesis)
        if err != nil {
            ioEntry.Status = EntryStatus_DEFERRED
        }
    }

    if err == nil {
        ioEntry.Status, err = ch.ValidateEntry(ioEntry.URID, &ioEntry.ChEntry, chMgr)
    }

    if err == nil {
        err = ch.MergeEntry(ioEntry)
    }


    chMgr.flow.LogErr(err, "error merging entry")


}





func (chMgr *ChMgr) FetchACC(
    inChannelID []byte,
) (*ChACC, error) {

    var acc *ChACC

    ch, err := chMgr.FetchChannel(inChannelID, nil)
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

    ch, err := chMgr.FetchChannel(plan.MemberRegistryChannelID[:], nil)
    if err == nil {
        return nil, err
    }

    memberRegistry := ch.(*ChMemberRegistry)

    return memberRegistry, nil
}


// FetchChannel returns the owning ChAgent for the given channel ID.
// If inGenesisEpoch is set, channel genesis is performed. 
func (chMgr *ChMgr) FetchChannel(
    inChannelID []byte,
    inGenesisEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    var (
        err error
    )

    chID := plan.GetChannelID(inChannelID)

    chMgr.lookupMutex.Lock()
    defer chMgr.lookupMutex.Unlock()
    chSt := chMgr.hotChannels[chID]

    // TODO: channel genesis dupes?

    // If the ch store isn't already loaded, try to load it.
    // To do that, we need to need to lookup it's ChStoreID
    if chSt == nil { 
        chSt, err = chMgr.loadChannel(inChannelID, inGenesisEpoch)
        if err == nil {

 
        }


        if err == nil {
            chMgr.hotChannels[chID] = chSt
        }  
    }
    
    // TODO: rebuild channel if db load fails
    if err != nil {
        chSt = nil
    }

    return chSt, err
}


func (chMgr *ChMgr) loadChannel(
    inChannelID []byte,
    inGenesisEpoch *pdi.ChannelEpoch,
) (ChAgent, error) {

    chSt := &ChStore{
        ChannelID: inChannelID,
    }

    opts := badger.DefaultOptions
    opts.Dir = path.Join(
        chMgr.HomePath,
        chMgr.fsNameEncoding.EncodeToString(inChannelID[:1]),
        chMgr.fsNameEncoding.EncodeToString(inChannelID),
    )
    opts.ValueDir = opts.Dir


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



func (chEntry *ChEntryInfo) AssignFromDecrytedEntry(
    inEntryInfo *pdi.EntryInfo,
    inAuthorSig []byte,
) {

    chEntry.TimeAuthored        = inEntryInfo.TimeAuthored
    chEntry.TimeAuthoredFrac    = inEntryInfo.TimeAuthoredFrac
    chEntry.EntryOp             = inEntryInfo.EntryOp
    chEntry.ChannelEpochID      = inEntryInfo.ChannelEpochID
    chEntry.AuthorMemberID      = inEntryInfo.AuthorMemberID
    chEntry.AuthorMemberEpoch   = inEntryInfo.AuthorMemberEpoch
    chEntry.BodyEncoding        = inEntryInfo.BodyEncoding
    chEntry.AuthorSig           = inAuthorSig

}





type ChSession struct {
    SessionID           int32

    MemeberSession      *MemberSession

    //ChStore             *ChStore

    //ChAdapter           ChAdapter

    // The currently open pipes 
    //Pipe                pservice.CommunityRepo_ChSessionPipeServer

    // Close the Inbox to end the channel session.
    // When closure is complete, the sessions msg outbox will be closed.
    ChMsgInbox          chan *plan.Block 
 
    ChMsgOutbox         chan *plan.Block

    log                 *log.Logger
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


CR (repo-wide) dbs:


Each channel has these dbs:
   - body db: URID => protobuf output from the ChannelProtocolClerk (allowing searching to be easier, etc)
        - optional -- allows the channel protocol to decide what it wants to keep around
    - entry db: URID => ChEntry
        - track entry status in a channel
*/



