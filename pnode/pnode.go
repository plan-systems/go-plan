

package pnode

import (

    //"fmt"
    "flag"
    //"log"
    "os"
    //"io"
    "io/ioutil"
    "strings"
    //"sync"
    //"time"
    //"sort"
    "encoding/hex"
    "encoding/json"
    //"encoding/base64"

    //"github.com/tidwall/redcon"

    "github.com/plan-tools/go-plan/plan"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"
    "crypto/rand"

    //"github.com/stretchr/testify/assert"

    //"github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/common/hexutil"

    "github.com/plan-tools/go-plan/pservice"

    "golang.org/x/net/context"

)

/*
const (
    DEBUG     = true
)


A community KeyID identifies a specific shared "community-global" symmetric key.
When a PLAN client starts a session with a pnode, the client sends the pnode her community-public keys.
PDIEntryCrypt.CommunityKeyID specifies which community key was used to encrypt PDIEntryCrypt.Header.
If/when an admin of a community issues a new community key,  each member is securely sent this new key via
the community key channel (where is key is asymmetrically sent to each member still "in" the community. 

When a PLAN client starts a new session with a pnode, the client sends the community keys for the session.  This allows
pnode to process and decrypt incoming PDIEntryCrypt entries from the storage medium (e.g. Ethereum).  Otherwise, pnode
has no ability to decrypt PDIEntryCrypt.Header.  A pnode could be configured to keep the community keychain even when there
are no open client PLAN sessions so that incoming entries can be processed.  Otherwise, incoming entries won't be processed
from the lowest level PDI storage layer.  Both configurations are reasonable depending on security preferences.

When a community admin initiates a community-key "rekey event", the newly generated community key is securely and individually
"sent" to each community member via the community's public key transfer channel. The new community key is encrypted using each member's public key.
When a pnode is processing an entry that it does not have a community key for, it will check the community's public key channel
for an entry for the current client's public key, it will send the client the encrypted community key.  The client uses its SKI
to decrypt the payload into the new community key.  This key is added to the user's SKI keychain and is sent back to pnode.


Recall that the pnode client has no ability to decrypt PDIEntryCrypt.Body if PDIEntryHeader.AccessChannelID isn't set for
community-public permissions.  This is fine since only PLAN clients with a





*/







// Pnode represents an instance of a running pnode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Pnode struct {
    CRbyID                      map[plan.CommunityID]*CommunityRepo

    config                      PnodeConfig

    ActiveSessions              SessionGroup

    BasePath                    string

}


// RuntimeSettings specifies settings that are solely associated with system load, performance, and resource allocation.
type RuntimeSettings struct {

    // Approx hard limit of the number of ChannelStores that are open/active moment to moment
    MaxOpenChannels             int                             `json:"maxOpenChannels"`

    // Number of seconds before an open ChannelStore will auto-close due to inactivity
    ChannelAutoCloseTimer       int                             `json:"channelAutoCloseTimer"`

    // Number of seconds before any pending writes to an open ChannelStore will auto-flush to disk. 
    // Note: A value of 0 means that writes are synchronous and could potentially lower performance.
    ChannelAutoFlushTimer       int                             `json:"channelAutoFlushTimer"`

    // Number of seconds of client session inactivity before the session is internally auto-ended,
    //    the auth token is no longer valid, and the client must start a new session.
    MaxAuthTokenInactivity      int                             `json:"maxAuthTokenInactivity"`
}


// PnodeConfig specifies all operating parameters if a pnode (PLAN's p2p/server node)
type PnodeConfig struct {
    Name                        string                          `json:"nodeName"`
    PnodeID                     hexutil.Bytes                   `json:"nodeID"`

    RepoList                    []CommunityRepoInfo             `json:"repoList"`

    DefaultFileMode             os.FileMode                     `json:"defaultFileMode"`

    RuntimeSettings             RuntimeSettings                 `json:"runtimeSettings"`

    GrpcNetworkName             string                          `json:"grpcNetwork"`
    GrpcNetworkAddr             string                          `json:"grpcAddr"`

    Version                     string                          `json:"version"`

}





const (

    // DefaultGrpcNetworkName is the default net.Listen() network layer name
    DefaultGrpcNetworkName      = "tcp"

    // DefaultGrpcNetworkAddr is the default net.Listen() local network address
    DefaultGrpcNetworkAddr      = ":50051"

    // CurrentPnodeVersion specifies the pnode version 
    CurrentPnodeVersion         = "0.1"
)


func init() {



}





func NewPnode() *Pnode {
    pn := &Pnode{}

    pn.config.DefaultFileMode = os.FileMode(0775)
    pn.config.GrpcNetworkName = DefaultGrpcNetworkName
    pn.config.GrpcNetworkAddr = DefaultGrpcNetworkAddr

    pn.config.RuntimeSettings.MaxOpenChannels          = 50
    pn.config.RuntimeSettings.ChannelAutoCloseTimer    = 60 * 60
    pn.config.RuntimeSettings.ChannelAutoFlushTimer    = 10
    pn.config.RuntimeSettings.MaxAuthTokenInactivity   = 60 * 60 * 24

    pn.CRbyID                   =  map[plan.CommunityID]*CommunityRepo{}
    pn.ActiveSessions.table     =  map[string]*ClientSession{}  

    return pn;
}




func (pn *Pnode) Init() error {

    basePath    := flag.String( "datadir",      "",         "Directory for config files, keystore, and community repos" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh pnode" )

    flag.Parse()

    if basePath != nil {
        pn.BasePath = *basePath
    } else {
        pn.BasePath = os.Getenv("HOME") + "/PLAN/pnode/"
    }

    var err error

    if ( *init ) {
        err = pn.InitOnDisk()
        if err != nil {
            return err
        }
    }

    err = pn.LoadConfigIn()
    if err != nil {
        return err
    }

    return nil
}




/*
// PnodeID identifies a particular pnode running as part of a PLAN community
type PnodeID                [32]byte

func (pnID PnodeID) MarshalJSON() ( []byte, error ) {
    bytesNeeded := base64.RawURLEncoding.EncodedLen( len(pnID) ) + 2
    outText := make( []byte, bytesNeeded, bytesNeeded )
    outText[0] = '"'
    base64.RawURLEncoding.Encode( outText[1:bytesNeeded-1], pnID[:] )
    outText[bytesNeeded-1] = '"'

    return outText, nil
}

func (pnID PnodeID) UnmarshalJSON( inText []byte ) error {
    _, err := base64.RawURLEncoding.Decode( pnID[:], inText )

    return err
}
*/


func (pn *Pnode) InitOnDisk() error {

    pn.config.PnodeID = make( []byte, plan.IdentityAddrSz )
    rand.Read( pn.config.PnodeID )

    return pn.WriteConfigOut()
}



func (pn *Pnode) LoadConfigIn() error {

    buf, err := ioutil.ReadFile( pn.BasePath + PnodeConfigFilename )
    if err == nil {
        err = json.Unmarshal( buf, &pn.config )
    }

    for i := range pn.config.RepoList {
        CRInfo := &pn.config.RepoList[i]
        
        CR := NewCommunityRepo( CRInfo, pn )

        pn.RegisterCommunityRepo( CR )
    }

    return err
}



func (pn *Pnode) RegisterCommunityRepo( CR *CommunityRepo ) {

    var communityID plan.CommunityID
    copy( communityID[:], CR.Info.CommunityID )

    pn.CRbyID[communityID] = CR

}



func (pn *Pnode) WriteConfigOut() error {

    os.MkdirAll( pn.BasePath, pn.config.DefaultFileMode )

    buf, err := json.MarshalIndent( &pn.config, "", "\t" )
    if err != nil {
        return err
    }

    err = ioutil.WriteFile( pn.BasePath + PnodeConfigFilename, buf, pn.config.DefaultFileMode )

    return err

}






func (pn *Pnode) CreateNewCommunity( inCommunityName string ) *CommunityRepo {

    pn.config.RepoList = append( pn.config.RepoList, CommunityRepoInfo{} )

    info := &pn.config.RepoList[len(pn.config.RepoList)-1]
    
    {
        info.CommunityName = inCommunityName
        info.CommunityID = make( []byte, plan.IdentityAddrSz )
        rand.Read( info.CommunityID )

        {
            var b strings.Builder
            remapCharset := map[rune]rune{
                ' ':  '-',
                '.':  '-',
                '?':  '-',
                '\\': '+',
                '/':  '+',
                '&':  '+',
            }
    
            for _, r := range strings.ToLower( info.CommunityName ) {
                if replace, ok := remapCharset[r]; ok {
                    if replace != 0 {
                        b.WriteRune(replace)
                    }
                } else {
                    b.WriteRune(r)
                }
            }
    
    
            info.RepoPath = b.String() + "-" + hex.EncodeToString( info.CommunityID[:4] ) + "/"
            info.ChannelPath = info.RepoPath + "ch/"
            info.TimeCreated = plan.Now()
            info.MaxPeerClockDelta = 60 * 25
        }

    }

    CR := NewCommunityRepo( info, pn )

    pn.RegisterCommunityRepo( CR )

    pn.WriteConfigOut()

    return CR

}



func NewClientSession(in *pservice.SessionRequest) *ClientSession {

    session := &ClientSession{
        AuthToken: GenRandomSessionToken(32),
        PrevActivityTime: plan.Now(),
        SessionRequest: in,
    }


    return session
}



// BeginSession implements pservice.PserviceServer
func (pn *Pnode) BeginSession(
    ctx context.Context,
    in *pservice.SessionRequest,
    ) (*pservice.SessionInfo, error) {

    var communityID plan.CommunityID
    communityID.AssignFrom(in.CommunityId)

    CR := pn.CRbyID[communityID]
    if CR == nil {
        return nil, plan.Errorf(nil, plan.CommunityNotFound, "community ID not found {ID:%v}", in.CommunityId)
    }

    session := NewClientSession(in)

    // TODO secuitry checks to prevent DoS
    pn.ActiveSessions.InsertSession(session)
    
    sessionInfo := &pservice.SessionInfo{
        SessionToken: session.AuthToken,
    }

	return sessionInfo, nil
}


// ReportStatus implements pservice.PserviceServer
func (pn *Pnode) ReportStatus( ctx context.Context, in *pservice.StatusQuery ) ( *pservice.StatusReply, error ) {
    session, err := pn.ActiveSessions.FetchSession(ctx)
    if err != nil {
        return nil, err
    }

    msg := "Hello, " + in.TestGreeting + "  token:"  + session.AuthToken
	return &pservice.StatusReply{ TestReply: msg }, nil
}

/*
// QueryChannels implements pservice.PserviceServer
func (pn *Pnode) QueryChannels( ctx context.Context, in *pservice.ChannelSearchParams ) ( *pservice.PDIChannelList, error ) {
	return &pservice.PDIChannelList{ }, nil
}

// OpenChannelEntryQuery implements pservice.PserviceServer
func (pn *Pnode) OpenChannelEntryQuery( in *pservice.ChannelEntryQuery, out pservice.Pservice_OpenChannelEntryQueryServer ) error {
	return nil
}

// PublishChannelEntry QueryChannels implements pservice.PserviceServer
func (pn *Pnode) PublishChannelEntry( ctx context.Context, in *pservice.PDIEntry ) ( *pservice.PStatus, error ) {
	return &pservice.PStatus{ }, nil
}


*/








