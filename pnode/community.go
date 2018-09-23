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

    "github.com/plan-tools/go-plan/pdi/StorageProviders/bolt"

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
    TimeCreated             plan.Time               `json:"time_created"`  

    // Max number of seconds that any two community peers could have different clock readings
    MaxPeerClockDelta       int64                   `json:"max_peer_clock_delta"`
}


// CommunityRepo wraps a community's data repository and responds to queries for the given community.
type CommunityRepo struct {
    Info                    *CommunityRepoInfo

    AbsRepoPath             string

    // This will move and is just a hack for now
    //activeSession           ClientSession

    DefaultFileMode         os.FileMode
    DirEncoding             *base64.Encoding

    storage                 pdi.StorageSession
    storageProvider         pdi.StorageProvider
    storageMsgs             <-chan *pdi.StorageMsg

    txnsToProcess          chan txnInProcess

    ParentPnode             *Pnode

    // Newly authored entries from active sessions on this pnode that are using this CommunityRepo.
    // These entries are first validated/processed as if they came off the wire, merged with the local db, and commited to the active storage sessions.
    authoredInbox           chan *pdi.EntryCrypt

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







func NewCommunityRepo(
    inInfo *CommunityRepoInfo,
    inParent *Pnode,
    ) *CommunityRepo {

    CR := &CommunityRepo{
        ParentPnode: inParent,
        Info: inInfo,
        DirEncoding: base64.RawURLEncoding,
        DefaultFileMode: inParent.Config.DefaultFileMode,
        txnsToProcess: make(chan txnInProcess),
    }
  

    if path.IsAbs(CR.Info.RepoPath) {
        CR.AbsRepoPath = CR.Info.RepoPath
    } else {
        CR.AbsRepoPath = path.Join(inParent.Params.BasePath, CR.Info.RepoPath)
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

    CS.channelDir = path.Join(CR.AbsRepoPath, "ch", CR.DirEncoding.EncodeToString(CS.ChannelID[:]))

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

func (CR *CommunityRepo) StartService() error {


    CR.storageProvider = bolt.NewProvider(
        path.Join(CR.AbsRepoPath, "Layer I"),
        CR.DefaultFileMode,
    )

    var err error
    CR.storage, err = CR.storageProvider.StartSession(
        CR.Info.CommunityID,
    )
    if err != nil {
        return err 
    }

    CR.storageMsgs = CR.storage.GetOutgoingChan()


    go func() {

        var entryBatch []*pdi.EntryCrypt

        for {
               
            var authoredEntry *pdi.EntryCrypt
            var storageMsg *pdi.StorageMsg

            // First, try drawing from the newly authored entry inbox (from currently connection community member sessions).
            select {
                case authoredEntry = <- CR.authoredInbox:
                    entryBatch = append(entryBatch, authoredEntry)
                    doneWithBatch := false
                    for ! doneWithBatch {
                        select {
                            case authoredEntry = <- CR.authoredInbox:
                                entryBatch = append(entryBatch, authoredEntry)
                            default:
                                doneWithBatch = true
                        }
                    }
                    CR.txnsToProcess <- txnInProcess{
                        entryBatch: entryBatch,
                    }
                    entryBatch = entryBatch[:0]


                // If no newly authored entries await processing, see if there's any incoming from the storage session.
                case storageMsg = <-CR.storageMsgs:
                    switch storageMsg.StorageOp {
                        
                        case pdi.OpRequestTxns:
                        case pdi.OpTxnReport:
                            for _, txn := range storageMsg.Txns {
                                entryBatch, err = txn.UnmarshalEntries(entryBatch)
                                if err != nil {
                                    // TODO: handle err better
                                    panic(err)
                                }
                                CR.txnsToProcess <- txnInProcess{
                                    entryBatch: entryBatch,
                                    parentTxnName: txn.TxnName,
                                }
                                entryBatch = entryBatch[:0]
                            }
                    }
            }
        }
    }()


    go func() {

        doNextEntry := make(chan bool)

 
        for {

            var eip entryInProcess
            eip.CR = CR

            tip, ok := <-CR.txnsToProcess
            if ! ok {
                break
            }

            for i, entry := range tip.entryBatch {
                eip.entry = entry
                eip.entryTxnIndex = i
                eip.parentTxnName = tip.parentTxnName

                eip.processAndMergeEntry(func (inErr *plan.Perror) {
                    if inErr != nil {
                        
                        eip.failedEntries = append(eip.failedEntries, failedEntry{
                            err: inErr, 
                            entry: eip.entry,
                            entryTxnIndex: eip.entryTxnIndex,
                            parentTxnName: eip.parentTxnName,
                            })
                        log.Printf("failed to process entry: %s", inErr)
                    }

                    doNextEntry <-true
                })

                // Block until the current entry finishes processing
                <- doNextEntry
            }

            // If there's no parent txn name, that's a signal the batch of entries needs to be committed to the community repo
            if len(tip.parentTxnName) == 0 {
                body := pdi.MarshalEntries(tip.entryBatch)
                _, err := CR.storage.CommitTxn(body)
                if err == nil {
                    // TODO: how do we handle this error.  make a local repo for failed txns to be retried?
                    panic(err)
                }
            }

        }

    }()

    return nil    
}



func (CR *CommunityRepo) PublishEntry( inEntry *pdi.EntryCrypt ) {

    //CR.entryInbox <- inEntry
}






func (CR *CommunityRepo) RevokeEntry( inHashnames []byte ) {

    
}









type failedEntry struct {
    err                     *plan.Perror
    entry                   *pdi.EntryCrypt
    entryTxnIndex           int
    parentTxnName           []byte
}




type txnInProcess struct {
    entryBatch      []*pdi.EntryCrypt
    parentTxnName   []byte
}





type entryInProcess struct {
    CR              *CommunityRepo    

    timeStart       plan.Time

    entry           *pdi.EntryCrypt
    entryTxnIndex   int
    parentTxnName   []byte
/*
    txnWS           []txnWorkspace
    entryBatch      []*pdi.EntryCrypt // 
    entryIndex      int               // This is the index number into entryBatch that is currently being processed
    entryTxn        pdi.StorageTxn */

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
func (eip *entryInProcess) unpackHeader(
    inOnCompletion func(*plan.Perror),
    ) {

    eip.timeStart = plan.Now()

    switch vers := eip.entry.GetEntryVersion(); vers {
        case pdi.EntryVersion_V0:
            //ws.skiVersion = ski.CryptSKIVersion
        default:
            inOnCompletion(plan.Errorf(nil, plan.BadPDIEntryFormat, "bad or unsupported PDI entry version {vers:%d}", vers))
            return
    }

    // The entry header is encrypted using one of the community keys.
    eip.skiSession.DispatchOp( 

        &ski.OpArgs {
            OpName: ski.OpDecryptFromCommunity,
            CryptoKeyID: plan.GetKeyID(eip.entry.CommunityKeyId),
            Msg: eip.entry.HeaderCrypt,
        }, 

        func(inRespose *plan.Block, inErr *plan.Perror) {
            if inErr != nil {
                inOnCompletion(inErr)
                return
            }

            err := eip.entryHeader.Unmarshal(inRespose.Content)
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
func (eip *entryInProcess) validateEntry() *plan.Perror {

    if eip.entryHeader.TimeAuthored < eip.CR.Info.TimeCreated.UnixSecs {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp earlier than community creation timestamp")
    }
    if eip.timeStart.UnixSecs - eip.entryHeader.TimeAuthored + eip.CR.Info.MaxPeerClockDelta < 0 {
        return plan.Error(nil, plan.BadTimestamp, "PDI entry has timestamp too far in the future")
    }

    perr := eip.CR.LookupMember(eip.entryHeader.AuthorMemberId, plan.MemberEpoch(eip.entryHeader.AuthorMemberEpoch), &eip.authorEpoch)
    if perr != nil {
        return perr
    }

    eip.entryHash = eip.entry.ComputeHash()

    perr = eip.skiProvider.VerifySignature( 
        eip.entry.Sig,
        eip.entryHash,
        eip.authorEpoch.PubSigningKey,
    )

    if perr != nil {
        return plan.Error(perr, plan.FailedToProcessPDIHeader, "PDI entry signature verification failed")
    }


    err := eip.prepChannelAccess()

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


func (eip *entryInProcess) prepChannelAccess() *plan.Perror {

    plan.Assert( eip.targetChFlags == 0 &&  eip.accessChFlags == 0, "channel store lock flags not reset" )

    targetChFlags := LockForWriteAccess
    accessChFlags := LockForReadAccess | CitedAsAccessChannel

    switch eip.entryHeader.EntryOp {
        case pdi.EntryOp_EDIT_ACCESS_GRANTS:
            targetChFlags |= CitedAsAccessChannel
    }

    // First lock the target channel
    var perr *plan.Perror
    eip.targetCh, perr = eip.CR.LockChannelStore(eip.entryHeader.ChannelId, targetChFlags)
    if perr != nil {
        return perr
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    eip.targetChFlags = targetChFlags


    // Step from newest to oldest epoch.
    var epochMatch *pdi.ChannelEpoch
    for i, epoch := range eip.targetCh.ChannelEpochs {
        if epoch.EpochId == eip.entryHeader.ChannelEpochId {
            if i > 0 {
                // TODO: ws.targetChannel.ChannelEpoch[i-1].EpochTransitionPeriod
                {

                    // TargetChannelEpochExpired
                }
            }
        }
    }
    if epochMatch == nil {
        return plan.Errorf(nil, plan.TargetChannelEpochNotFound, "epoch 0x%x for target channel 0x%x not found", eip.entryHeader.ChannelEpochId, eip.entryHeader.ChannelId)
    }

    // Lookup the latest 
    eip.accessCh, perr = eip.CR.LockChannelStore(epochMatch.AccessChannelId, accessChFlags)
    if perr != nil {
        return perr
    }

    // At this point, ws.targetChannel is locked according to targetChFlags, so we need to track that
    eip.accessChFlags = accessChFlags

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




func (eip *entryInProcess) mergeEntry(
    inOnCompletion func(*plan.Perror),
    ) {


}






func (eip *entryInProcess) processAndMergeEntry( 
    inOnCompletion func(*plan.Perror),
    ) {

    eip.unpackHeader( func(inErr *plan.Perror) {
        if inErr != nil {
            inOnCompletion(inErr)
        }

        perr := eip.validateEntry()
        if perr != nil {
            inOnCompletion(perr)
        }


        perr = eip.prepChannelAccess()
        if perr != nil {
            inOnCompletion(perr)
        }

        eip.mergeEntry(inOnCompletion)
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
