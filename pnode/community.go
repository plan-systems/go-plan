package pnode


import (
    //"database/sql"
    //"fmt"
    //"log"
    "os"
    //"io"
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
    //"github.com/plan-tools/go-plan/ski"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"

    "github.com/ethereum/go-ethereum/common/hexutil"

)




// IdentityInfo represents a member's community-public-facing information
type IdentityInfo struct {
    IdentityRev         int32

    MemberID            plan.IdentityAddr

    SigningPubKey       []byte
    EncryptionPubKey    []byte

    HomeChannel         plan.ChannelID
}



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
    activeSession           ClientSession

    DefaultFileMode         os.FileMode
    DirEncoding             *base64.Encoding

    // Incoming encrypted entries "off the wire" that are ready to be processed (and written to local community repo)
    entriesToProcess        chan *pdi.EntryCrypt

    ParentPnode             *Pnode

    // deamonSKIs makes it possible for community public encrypted data to be decrypted, even when there are NO
    //     client sessions open.  Or, a community repo may have its security settings such that the community keyring
    //     is dropped when there are no more active client sessions open.
    //deamonSKIs              []plan.SyncSKI

    // Includes both open and loaded channels
    loadedChannels          ChannelStoreGroup

    // This group is checked first when looking up a channel and is a group of channels that are open/hot in one way or another.
    openChannels            ChannelStoreGroup

}





const (
    AuthorHasReadAccess        = 0x01
    AuthorHasWriteAccess       = 0x02
    AuthorHasOwnAccess         = 0x04
)

type AuthorAccessFlags  uint32



/*
There are 6 user channel access states:

I) Channel community-public or private?
    (a) community-public -- entry body encrypted with a community (symmetric) key and the listed AccessChannelID specifies
                            what default permissions are and what users are excluded from that (e.g. default read-only but
                            users Alice and Bob have write access, while only Bob has own access)
    (b) private          -- entry body encrypted with the channel key "sent" to users via AccessChannelID.  Users are granted the key for
                            read access, and potentially an additional "write" or "own" flag (that pnodes verify/enforce)

II) AccessChannelID privilege levels:
    (1) read-only        -- User have symmetric key access but pnodes reject posts if the author only had read permissions 
    (2) read + write     -- Users have additional post flag set (so pnodes see a given author is permitted to post a new entry)
    (3) owner            -- Can read, write, and grant others users read and write access privs.


*/

type FetchChannelStoreFlags int32
const (
    ReadingFromChannel          = 0x01
    PostingToChannel            = 0x02
    AdministeringChannel        = 0x04

    IsAccessChannel             = 0x10

    LoadIfNeeded                = 0x30
)







func NewCommunityRepo( inInfo *CommunityRepoInfo, inParent *Pnode ) *CommunityRepo {

    CR := new( CommunityRepo )
    CR.Info = inInfo
    CR.DirEncoding = base64.RawURLEncoding

    // Runtime support
    CR.entriesToProcess = make( chan *pdi.EntryCrypt, 32 )
    CR.DefaultFileMode = inParent.config.DefaultFileMode
    CR.ParentPnode = inParent

    return CR
}





func (CR *CommunityRepo) FetchChannelStore(
    inChannelID []byte,
    inChannelEpoch uint64,
    inFlags FetchChannelStoreFlags,
    ) (*ChannelStore, *plan.Perror) {

    var channelID plan.ChannelID
    channelID.AssignFrom(inChannelID)

    CR.loadedChannels.RLock()
    CS := CR.loadedChannels.table[channelID]
    CR.loadedChannels.RUnlock()

    var err *plan.Perror
    var hasWriteLock bool

    if CS == nil && ( inFlags & LoadIfNeeded ) != 0 {

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
        if ( inFlags & PostingToChannel ) != 0 {
            if ! hasWriteLock {
                CS.Lock()
            }
        } else if ( inFlags & ReadingFromChannel ) != 0 {
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
    CS.ChannelID.AssignFrom( inChannelID )

    CS.Lock()

    CS.channelDir = CR.Info.ChannelPath + CR.DirEncoding.EncodeToString( CS.ChannelID[:] ) + "/"

    if inCreateNew {

        err := os.Mkdir( CS.channelDir, CR.DefaultFileMode )
        if err != nil {
            return nil, plan.Error(err, plan.FailedToLoadChannelFromDisk, "failed to create channel directory" )
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


    if CS.ChannelAdmin.AccessChannelId != nil {

        CS.ACStore = new(ACStore)

        if inCreateNew {

        } else {

        }
    }

    return CS, nil
}


func (CR *CommunityRepo) LookupMember(
    inMemberID []byte,
    inMemberEpoch plan.MemberEpoch,
    outInfo *IdentityInfo,
    ) *plan.Perror {

    return nil
}


func (CR *CommunityRepo) StartService() {

    go func() {
        var ws entryWorkspace
        ws.CR = CR

        doNextEntry := make(chan bool)

        for entry := range CR.entriesToProcess {
            ws.entry = entry

            ws.processAndStoreEntry(func (*plan.Perror){
                doNextEntry <-true;
            })

            // Block until the current entry finishes processing
            <- doNextEntry
            
        }
    }()
        
}



func (CR *CommunityRepo) PublishEntry( inEntry *pdi.EntryCrypt ) {

    CR.entriesToProcess <- inEntry
}






func (CR *CommunityRepo) RevokeEntry( inHashnames []byte ) {

    
}


