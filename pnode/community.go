package pnode


import (
    //"database/sql"
    //"fmt"
    "log"
    "os"
    "path"
    "io/ioutil"
    //"strings"
    "sync"
    //"time"
    //"sort"
    //"encoding/hex"
    "encoding/json"
    "encoding/base64"

    //"github.com/tidwall/redcon"

    "github.com/plan-tools/go-plan/pdi"
    "github.com/plan-tools/go-plan/plan"
    "github.com/plan-tools/go-plan/ski"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"

    "github.com/ethereum/go-ethereum/common/hexutil"

)





// ChannelStoreGroup takes a plan.ChannelID and hands back a ChannelStore while ensuring concurrency safety.
type ChannelStoreGroup struct {
    sync.RWMutex

    table                   map[plan.ChannelID]*ChannelStore
}






type CommunityRepoInfo struct {
    CommunityName           string                  `json:"community_name"`
    CommunityID             hexutil.Bytes           `json:"community_id"`
    RepoPath                string                  `json:"repo_path"`
    ChannelPath             string                  `json:"channel_path"`
    TimeCreated             plan.Time               `json:"time_created"`  

    // Max number of seconds that any two community peers could have different clock readings
    MaxPeerClockDelta       int64                   `json:"max_peer_clock_delta"`
}


// CommunityRepo wraps a community's data repository and responds to queries for the given community.
type CommunityRepo struct {
    Info                    *CommunityRepoInfo

    // This will move and is just a hack for now
    //activeSession           ClientSession

    DefaultFileMode         os.FileMode
    DirEncoding             *base64.Encoding

    storage                 pdi.StorageSession
    storageProvider         pdi.StorageProvider
    storageMsgInbox         chan pdi.StorageMsg

    ParentPnode             *Pnode

    // Newly authored entries from active sessions on this pnode that are using this CommunityRepo.
    // These entries are first validated/processed as if they came off the wire, merged with the local db, and commited to the active storage sessions.
    authoredInbox           chan pdi.EntryCrypt

    // deamonSKIs makes it possible for community public encrypted data to be decrypted, even when there are NO
    //     client sessions open.  Or, a community repo may have its security settings such that the community keyring
    //     is dropped when there are no more active client sessions open.
    //deamonSKIs              []plan.SyncSKI

    // Includes both open and loaded channels
    loadedChannels          ChannelStoreGroup

    // This group is checked first when looking up a channel and is a group of channels that are open/hot in one way or another.
    openChannels            ChannelStoreGroup

}


    
type CommunityRules struct {

    MaxMemberAliasChangesPerMonth int //= 30

}



/*
const (
    AuthorHasReadAccess        = 0x01
    AuthorHasWriteAccess       = 0x02
    AuthorHasOwnAccess         = 0x04
)
*/


/*
There are two sources of access control information, both stored in a channel's owning access channel.
   (1)  Default ChannelAccess levels, and
   (2)  ChannelAccess grants given to explicit community members. 

Note that evne access channels follow the same plumbing, offering a powerful but compact hierarchal permissions schema.

Any channel (or access channel) can either be community-public or private.
    (a) community-public -- entry body encrypted with a community key and the owning access channel specifies
                            what permissions are and what users have grants outside of that (e.g. default READ_ACCESS,
                            users Alice and Bob have READWRITE_ACCESS access, Charlie is set for NO_ACCESS, and Daisy has MODERATOR_ACCESS.
    (b) private          -- entry body encrypted with the channel key "sent" to users via the channel's access channel.  Like with (a), 
                            members are granted explicit access levels (listed in ChannelAccess)

*/

type LoadChannelStoreFlags int32
const (
    LockForReadAccess    LoadChannelStoreFlags = 0x01
    LockForWriteAccess   LoadChannelStoreFlags = 0x02
    CitedAsAccessChannel LoadChannelStoreFlags = 0x10
)







func NewCommunityRepo( inInfo *CommunityRepoInfo, inParent *Pnode ) *CommunityRepo {

    CR := &CommunityRepo{
        ParentPnode: inParent,
        Info: inInfo,
        DirEncoding: base64.RawURLEncoding,
        DefaultFileMode: inParent.config.DefaultFileMode,
        storageMsgInbox: make(chan pdi.StorageMsg, 8),   // should this be buffered!?
    }
  
    return CR
}

/*
ws.targetChannel, perr = ws.CR.LockChannelStoreForOp(ws.entryHeader)

func (CR *CommunityRepo) LockChannelStoreForOp(
    inChannelID []byte,
    ) (*ChannelStore, *plan.Perror) {

*/




func (CR *CommunityRepo) LockChannelStore(
    inChannelID []byte,
    inFlags LoadChannelStoreFlags,
    ) (*ChannelStore, *plan.Perror) {


    channelID := plan.GetChannelID(inChannelID)

    CR.loadedChannels.RLock()
    CS := CR.loadedChannels.table[channelID]
    CR.loadedChannels.RUnlock()

    var err *plan.Perror
    var hasWriteLock bool

    if CS == nil {

        CS, err = CR.LoadChannelStore( inChannelID, false )

        // If we errored out, backout
        if CS == nil || err != nil {
            CS = nil
        } else {

            CR.loadedChannels.Lock()
            CR.loadedChannels.table[CS.ChannelID] = CS
            CR.loadedChannels.Unlock()

            hasWriteLock = true
        }
    }

    if err == nil {
        if ( inFlags & LockForWriteAccess ) != 0 {
            if ! hasWriteLock {
                CS.Lock()
            }
        } else if ( inFlags & LockForReadAccess ) != 0 {
            if hasWriteLock {
                CS.Unlock()
            }
            CS.RLock()
        } 
    }


    return CS, err
}





func (CR *CommunityRepo) LoadChannelStore(
    inChannelID []byte, 
    inCreateNew bool,
    ) (*ChannelStore, *plan.Perror) {

    CS := new( ChannelStore )
    CS.ChannelID = plan.GetChannelID(inChannelID)

    CS.Lock()

    CS.channelDir = CR.Info.ChannelPath + CR.DirEncoding.EncodeToString( CS.ChannelID[:] ) + "/"

    if inCreateNew {

        err := os.Mkdir( CS.channelDir, CR.DefaultFileMode )
        if err != nil {
            return nil, plan.Error(err, plan.FailedToLoadChannelFromDisk, "failed to create channel directory")
        }
    
        buf, err := json.Marshal( &CS.ChannelInfo )
        if err == nil {
            err = ioutil.WriteFile(CS.channelDir + ChannelPropertiesFilename, buf, CR.DefaultFileMode)
        }
        if err != nil {
            return nil, plan.Error(err, plan.FailedToLoadChannelFromDisk, "failed to create channel properties file")
        }

    } else {

        buf, err := ioutil.ReadFile(CS.channelDir + ChannelPropertiesFilename)
        if err == nil {
            err = json.Unmarshal(buf, &CS.ChannelInfo)
        }
        if err != nil {
            if os.IsNotExist(err) {
                return nil, plan.Error(err, plan.ChannelNotFound, "channel not found in repo")
            } else {
                return nil, plan.Error(err, plan.FailedToLoadChannelFromDisk, "failed to read channel properties file")
            }
        }
 
    }

/*
    if CS.ChannelEpoch.AccessChannelId != nil {

        CS.ACStore = new(ACStore)

        if inCreateNew {

        } else {

        }
    }
*/
    return CS, nil
}


func (CR *CommunityRepo) LookupMember(
    inMemberID []byte,
    inMemberEpoch plan.MemberEpoch,
    outInfo *pdi.MemberEpoch,
    ) *plan.Perror {

    return nil
}



/*
New txns appear in 1 of 2 possible ways to a pnode:
    (1) pdi.StorageSession() reports a new StorageMsg
        - For each contained StorageTxn, StorageTxn.Body is deserialized into one or more EntryCrypts
        - Each entry is merged into appropriate repo 
            - dupes are ignored
            - illegal/invalid entries are dropped or logged into a reject pool
        - For each channel all an active session has a query open for, send out new entries on that queru
    (2) An active community memebr session on a pnode submits a newly authored and signed EntryCrypt
        - *same* exact steps above AND a new StorageTxn is submitted to the community's active StorageSession(s)
        - The listening channels get entry txn status as it updates.
        
A StorageTxn contains a single plan.Block.   
Given that Block's codec info, it's expands into a sequence of zero or more EntryCrypt. 
    - This means an EntryCrypt is addressed by:
        1) its commit time (uint64)
        2) it's StorageTxn name  
    - Every entry would have *two* separate hashnames (the parent StorageTxn hashname and one to identify the entry)
        => Two BSTs would be needed, yikes
If one entry (max) per StorageTxn
    + An entry's hashname + commit time could be used universially and uniquely
    + An EntryCrypt's hash could be used as the txn hashname
*/

func (CR *CommunityRepo) StartService() {

    CR.storageProvider = NewBoltStorage(
        path.Join(CR.Info.RepoPath, "bolt"),
        CR.DefaultFileMode,
    )

    CR.storage = CR.storageProvider.StartSession(
        CR.Info.CommunityID,
        CR.storageMsgInbox,


    )

 






    go func() {
        var ws entryWorkspace
        ws.CR = CR

        doNextEntry := make(chan bool)

        for {
               
            var authoredEntry *pdi.EntryCrypt
            var storageMsg *pdi.StorageMsg

            // First, draw from the newly authored entry inbox (from currently connection community member sessions).
            select {
                case authoredEntry = <-CR.authoredInbox:

                // If no newly authored entries await processing, see if there's any incoming from the storage session.
                default:
                    
                    select {

                        case authoredEntry = <-CR.authoredInbox:

                        case storageMsg = <-CR.storageMsgInbox:
                    }

            }

            if authoredEntry != nil {
                ws.entry = authoredEntry
                ws.storageTxn = pdi.StorageTxn{
                    TxnStatus: TxnStatus_AWAITING_COMMIT
                }
            } else {
                extract

            }

            // Attempt to deserialize StorageTxn.Body into a EntryCrypt
One entry per StorageTxn, or one StorageTxn per StorageMSg?
Likeing the idea of singylar storage Txns, containing one or more entries.  problem is that entries won't have unique names
- solution: have sorted txn hashnames, where each hashname has a row for each contained entry (the entry's hashname relies on the sig)

            ws.entry = entry

            ws.processAndMergeEntry(func (inErr *plan.Perror) {
                if inErr != nil {
                    ws.failedEntries = append(ws.failedEntries, failedEntry{inErr, ws.entry})
                    log.Printf("failed to process entry: %s", inErr)
                }

                doNextEntry <-true;
            })

            // Block until the current entry finishes processing
            <- doNextEntry
            
        }
    }()
        
}



func (CR *CommunityRepo) PublishEntry( inEntry *pdi.EntryCrypt ) {

    CR.entryInbox <- inEntry
}






func (CR *CommunityRepo) RevokeEntry( inHashnames []byte ) {

    
}







type failedEntry struct {
    err                     *plan.Perror
    entry                   *pdi.EntryCrypt
}



type txnWorkspace {
    CR              *CommunityRepo    

    entryIndex      int               // This is the index currently being processed
    entryBatch      []*pdi.EntryCrypt // All the entries contained (or to be contained) in .txn
    txn             pdi.StorageTxn    // Host/Container of each entry in .entryBatch[:] 

}



// entryWorkspace is a workspace used to pass around
type entryWorkspace struct {
    CR              *CommunityRepo    

    timeStart       plan.Time

    entry           *pdi.EntryCrypt

    entryBatch      []*pdi.EntryCrypt // 
    entryIndex      int               // This is the index number into entryBatch that is currently being processed
    entryTxn        pdi.StorageTxn

    entryHash       []byte
    entryHeader     pdi.EntryHeader
    entryBody       plan.Block
    
    authorEpoch     pdi.MemberEpoch

    failedEntries   []failedEntry


    skiSession      ski.Session
    skiProvider     ski.Provider

    accessCh        *ChannelStore
    accessChFlags   LoadChannelStoreFlags

    targetCh        *ChannelStore
    targetChFlags   LoadChannelStoreFlags

}




// internal: unpackHeader
//   decrypts and deserializes a pdi header
func (ws *entryWorkspace) unpackHeader(
    inOnCompletion func(*plan.Perror),
    ) {

    ws.timeStart = plan.Now()

    switch vers := ws.entry.GetEntryVersion(); vers {
        case pdi.EntryVersion_V0:
            //ws.skiVersion = ski.CryptSKIVersion
        default:
            inOnCompletion(plan.Errorf(nil, plan.BadPDIEntryFormat, "bad or unsupported PDI entry version {vers:%d}", vers))
            return
    }

    // The entry header is encrypted using one of the community keys.
    ws.skiSession.DispatchOp( 

        &ski.OpArgs {
            OpName: ski.OpDecryptFromCommunity,
            CryptoKeyID: plan.GetKeyID(ws.entry.CommunityKeyId),
            Msg: ws.entry.HeaderCrypt,
        }, 

        func(inRespose *plan.Block, inErr *plan.Perror) {
            if inErr != nil {
                inOnCompletion(inErr)
                return
            }

            err := ws.entryHeader.Unmarshal(inRespose.Content)
            if err != nil {
                inOnCompletion(plan.Error(err, plan.FailedToProcessPDIHeader, "failed to unmarshal PDI header"))
                return
            }

            // At this point, ws.entryHeader is ready for use
            inOnCompletion(nil)
        },
    )

}



// internal: validateEntry
//   before we write to the pnode, we need to verify the author is
//   valid and that they had permissions to do the things the entry wants to do. 
//   note that because permissions are immutable at a point in time, it doesn't matter
//   when we check permissions if they're changed later -- they'll
//   always be the same for an entry at a specific point in time.
func (ws *entryWorkspace) validateEntry() *plan.Perror {

    if ws.entryHeader.TimeAuthored < ws.CR.Info.TimeCreated.UnixSecs {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp earlier than community creation timestamp")
    }
    if ws.timeStart.UnixSecs - ws.entryHeader.TimeAuthored + ws.CR.Info.MaxPeerClockDelta < 0 {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp too far in the future")
    }

    perr := ws.CR.LookupMember(ws.entryHeader.AuthorMemberId, plan.MemberEpoch(ws.entryHeader.AuthorMemberEpoch), &ws.authorEpoch)
    if perr != nil {
        return perr
    }

    ws.entryHash = ws.entry.ComputeHash()

    perr = ws.skiProvider.VerifySignature( 
        ws.entry.Sig,
        ws.entryHash,
        ws.authorEpoch.PubSigningKey,
    )

    if perr != nil {
        return plan.Error(perr, plan.FailedToProcessPDIHeader, "PDI entry signature verification failed")
    }


    err := ws.prepChannelAccess()

    // At this point, the PDI entry's signature has been verified
    return err

}


 /*
     defer {
        switch {
        case ( targetChFlags & LockForReadAccess ) != 0:
            ws.targetChannel.RUnlock()
        case ( targetChFlags & LockForWriteAccess ) != 0:
            ws.targetChannel.Unlock()
        }
}*/


type entryAccessReqs struct {
    minAccessLevel          pdi.ChannelAccess
    canEditOthersEntries    bool
}


func (ws *entryWorkspace) prepChannelAccess() *plan.Perror {

    plan.Assert( ws.targetChFlags == 0 &&  ws.accessChFlags == 0, "channel store lock flags not reset" )

    targetChFlags := LockForWriteAccess
    accessChFlags := LockForReadAccess | CitedAsAccessChannel

    switch ws.entryHeader.EntryOp {
        case pdi.EntryOp_EDIT_ACCESS_GRANTS:
            targetChFlags |= CitedAsAccessChannel
    }

    // First lock the target channel
    var perr *plan.Perror
    ws.targetCh, perr = ws.CR.LockChannelStore(ws.entryHeader.ChannelId, targetChFlags)
    if perr != nil {
        return perr
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    ws.targetChFlags = targetChFlags


    // Step from newest to oldest epoch.
    var epochMatch *pdi.ChannelEpoch
    for i, epoch := range ws.targetCh.ChannelEpochs {
        if epoch.EpochId == ws.entryHeader.ChannelEpochId {
            if i > 0 {
                // TODO: ws.targetChannel.ChannelEpoch[i-1].EpochTransitionPeriod
                {

                    // TargetChannelEpochExpired
                }
            }
        }
    }
    if epochMatch == nil {
        return plan.Errorf(nil, plan.TargetChannelEpochNotFound, "epoch 0x%x for target channel 0x%x not found", ws.entryHeader.ChannelEpochId, ws.entryHeader.ChannelId)
    }

    // Lookup the latest 
    ws.accessCh, perr = ws.CR.LockChannelStore(epochMatch.AccessChannelId, accessChFlags)
    if perr != nil {
        return perr
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    ws.accessChFlags = accessChFlags

/*
    // Ops such as REMOVE_ENTRIES and SUPERCEDE_ENTRY
    perr = ws.targetCh.FetchRelevantEntriesForOp()

    access := ws.accessCh.LookupAccessForAuthor(ws.entryHeader.AuthorMemberId)

    reqs := entryAccessReqs{

    }
    switch ws.entryHeader.EntryOp {
    case POST_NEW_CONTENT:
        reqs.minAccessLevel = READWRITE_ACCESS
        case pdi.EntryOp_EDIT_ACCESS_GRANTS:
            targetChFlags |= CitedAsAccessChannel
    }
*/


/*
      // Fetch and lock the data container for the cited access channel, checking all security permissions
    ws.targetChannel, err = ws.CR.LockChannelStoreForOp(ws.entryHeader)
    if err != nil {
        return err
    }

    accessLevel, err := ws.targetChannel.AccessLevelForMember(ws.entryHeader.
    var 
    for i, chEpoch := range ws.targetChannel.ChannelEpochs {
        if chEpoch.EpochId == ws.entryHeader.ChannelEpochId {
            for 
        }
    }

    fetchFlags := LockForReadAccess

    switch ( ws.entryHeader.EntryOp ) {

        case 
            EntryOp_UPDATE_ACCESS_GRANTS,
            EntryOp_EDIT_CHANNEL_EPOCH:
            
            fetchFlags = LockForWriteAccess
    }

    fetchFlags |= IsAccessChannel
    ws.targetChannel, err = ws.CR.FetchChannelStore(
        ws.targetChannel.ChannelEpoch.AccessChannelId, 
        ws.entryHeader.ChannelEpoch,
        fetchFlags )
    

    CitedAsAccessChannel

/*
    if ws.targetChannel == nil {
        return plan.Errorf(err, plan.AccessChannelNotFound, "channel 0x%x not found", ws.entryHeader.ChannelId )
    }

    if ws.targetChannel.ACStore == nil {
        return plan.Errorf(nil, plan.NotAnAccessChannel, "invalid channel 0x%x", ws.entryHeader.ChannelId )
    }

    // TODO: do all of ACStore checking!
*/
    return nil
}




func (ws *entryWorkspace) mergeEntry(
    inOnCompletion func(*plan.Perror),
    ) {


}






func (ws *entryWorkspace) processAndMergeEntry( 
    inOnCompletion func(*plan.Perror),
    ) {

    ws.unpackHeader( func(inErr *plan.Perror) {
        if inErr != nil {
            inOnCompletion(inErr)
        }

        perr := ws.validateEntry()
        if perr != nil {
            inOnCompletion(perr)
        }


        perr = ws.prepChannelAccess()
        if perr != nil {
            inOnCompletion(perr)
        }

        ws.mergeEntry(inOnCompletion)
    })
   

}   



/*
    entry := new( plan.PDIEntry )
    entry.PDIEntryCrypt = ioEntry

    var err error

    entry.HeaderBuf, err = CR.decryptCommunityData( ioEntry.CommunityKeyID, ioEntry.HeaderCrypt )
    if err != nil {
        return err
    }

    // De-serialize inEntry.HeaderBuf into inEntry.Header
    entry.Header = new( plan.PDIEntryHeader )
    err = rlp.DecodeBytes( entry.HeaderBuf, entry.Header )
    if err != nil {
        return err
    }

    // Used in various places
    ioEntry.Hash = new( plan.PDIEntryHash )
    ioEntry.ComputeHash( ioEntry.Hash )

    // Now that we've decrypted and de-serialized the header, we can verify the entry's signature
    err = CR.VerifySig( ioEntry )
    if err != nil {
        return err
    }

    // Fetch (or load and fetch) the ChannelStore associated with the given channel
    CS, err := CR.GetChannelStore( &entry.Header.ChannelID, PostingToChannel | LoadIfNeeded )

    verb := entry.Header.Verb
    switch ( verb ) {

        case plan.PDIEntryVerbPostEntry:

            // First, we must validate the access channel cited by the header used by the author to back permissions for posting this entry.
            // This checks that the author didn't use an invalid or expired access channel to post this entry.  Once we validate this, 
            //    we can trust and use that access channel to check permissions further.
            err = CS.ValidateCitedAccessChannel( entry.Header );

            err = CR.VerifyWriteAccess( CS, entry.Header )
            if err != nil {
                return err
            }

            err := CS.WriteEntryToStorage( entry )
            if err != nil {
                return err
            }

        case plan.PDIEntryVerbChannelAdmin:

            // In general, if the channel already exists, it's an error.  Howeever we need to check if this entry 
            if CS != nil {
                //err = plan.Error( )
            }

        default:
            plan.Assert( false, "Unhandled verb" )

    } 



    return err

}





// VerifyAccess checks that the given PDI Entry has the proper permissions to do what it says it wants to do and that
//    the AccessChannelID cited is in fact a valid access channel to cite (given the timestamp of the entry, etc)
func (CR *CommunityRepo) VerifyWriteAccess( CS *ChannelStore, inHeader *plan.PDIEntryHeader ) error {

    // Get/Load/Create the data structure container for the cited access channel
    AC, _ := CR.GetChannelStore( &inHeader.AccessChannelID, IsAccessChannel | ReadingFromChannel | LoadIfNeeded )
    if AC == nil {
        return plan.Errorf( AccessChannelNotFound, "cited access channel 0x%x not found", inHeader.AccessChannelID )
    }
    if AC.ACStore == nil {
        return plan.Errorf( NotAnAccessChannel, "cited access channel 0x%x not actually an access channel", inHeader.AccessChannelID )
    }

    // Entries posted to a channel cite (and use) the latest/current AccessChannelID associated with the channel.
    // ...but pnodes must check this!


    {
        access := AC.ACStore.AccessByAuthor[inHeader.Author]
        if ( access & AuthorHasWriteAccess ) == 0 {
            return plan.Error( AuthorLacksWritePermission, "Author does not have write access to channel" )
        }
    }

    return nil

}





*/
