

package pnode

import (

    //"fmt"
    "log"
    "os"
    //"io"
    "io/ioutil"
    "strings"
    //"sync"
    //"time"
    "net"
    "encoding/hex"
    "encoding/json"
    "encoding/base64"

    //"github.com/tidwall/redcon"

    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    //"github.com/plan-systems/go-plan/ski/Providers/nacl"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"
    "crypto/rand"

    //"github.com/stretchr/testify/assert"


    "github.com/plan-systems/go-plan/pservice"

    //"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

    //"github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/common/hexutil"

    "golang.org/x/net/context"

)



/*
const (
    DEBUG     = true
)


When a PLAN client starts a new session with a pnode, the client sends the community keys for the session.  This allows
pnode to process and decrypt incoming PDIEntryCrypt entries from the storage medium (e.g. Ethereum).  Otherwise, pnode
has no ability to decrypt PDIEntryCrypt.Header.  A pnode could be configured to keep the community keychain even when there
are no open client PLAN sessions so that incoming entries can be processed.  Otherwise, incoming entries won't be processed
from the lowest level PDI storage layer.  Both configurations are reasonable depending on security preferences.


Recall that the pnode client has no ability to decrypt PDIEntryCrypt.Body if PDIEntryHeader.AccessChannelID isn't set for
community-public permissions.  This is fine since only PLAN clients with a





*/







// Pnode represents an instance of a running pnode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Pnode struct {
    CRbyID                      map[plan.CommunityID]*CommunityRepo

    Config                      PnodeConfig

    ActiveSessions              SessionGroup

    Params                      PnodeParams

    FSNameEncoding              *base64.Encoding
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


type PnodeParams struct {
    BasePath          string
    ClientStoragePath string 
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





func NewPnode(inParams PnodeParams) *Pnode {
    pn := &Pnode{
        FSNameEncoding: base64.RawURLEncoding,
    }

    pn.Config.DefaultFileMode = os.FileMode(0775)
    pn.Config.GrpcNetworkName = DefaultGrpcNetworkName
    pn.Config.GrpcNetworkAddr = DefaultGrpcNetworkAddr

    pn.Config.RuntimeSettings.MaxOpenChannels          = 50
    pn.Config.RuntimeSettings.ChannelAutoCloseTimer    = 60 * 60
    pn.Config.RuntimeSettings.ChannelAutoFlushTimer    = 10
    pn.Config.RuntimeSettings.MaxAuthTokenInactivity   = 60 * 60 * 24

    pn.CRbyID                   =  map[plan.CommunityID]*CommunityRepo{}
    pn.ActiveSessions.table     =  map[string]*ClientSession{}  

    pn.Params = inParams

    return pn;
}



// Startup does basic loading from disk etc
func (pn *Pnode) Startup(inFullInit bool) error {

    var err error

    if inFullInit {
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



// Run is used like main()
func (pn *Pnode) Run() {


    // For each community repo a pnode is hosting, start service for that community repo.
    // By "start service" we mean that each PDI layer that repo is configured with starts up (e.g. Ethereum private distro) such that:
    //     (a) new PDI entries *from* that layer are handed over process processing
    //     (b) newly authored entries from the PLAN client are handed over to the PDI layer to be replicated to other nodes 
    //         also carrying that community.
    for _, CR := range pn.CRbyID {
        CR.StartService()
    }

    {
        lis, err := net.Listen(pn.Config.GrpcNetworkName, pn.Config.GrpcNetworkAddr)
        if err != nil {
            log.Fatalf( "failed to listen: %v", err )
        }
        grpcServer := grpc.NewServer()
        pservice.RegisterPnodeServer(grpcServer, pn)
        
        // Register reflection service on gRPC server.
        reflection.Register( grpcServer )
        if err := grpcServer.Serve( lis ); err != nil {
            log.Fatalf( "failed to serve: %v", err )
        }
    }
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

    pn.Config.PnodeID = make([]byte, plan.CommunityIDSz)
    rand.Read(pn.Config.PnodeID)

    return pn.WriteConfigOut()
}


func (pn *Pnode) LoadConfigIn() error {

    buf, err := ioutil.ReadFile(pn.GetConfigPathname())
    if err == nil {
        err = json.Unmarshal(buf, &pn.Config)
    }

    for i := range pn.Config.RepoList {
        CRInfo := &pn.Config.RepoList[i]
        
        CR := NewCommunityRepo(CRInfo, pn)

        pn.RegisterCommunityRepo( CR )
    }

    return err
}



func (pn *Pnode) RegisterCommunityRepo( CR *CommunityRepo ) {

    var communityID plan.CommunityID
    copy(communityID[:], CR.Info.CommunityID)

    pn.CRbyID[communityID] = CR

}



func (pn *Pnode) WriteConfigOut() error {

    os.MkdirAll(pn.Params.BasePath, pn.Config.DefaultFileMode)

    buf, err := json.MarshalIndent( &pn.Config, "", "\t" )
    if err != nil {
        return err
    }

    err = ioutil.WriteFile(pn.GetConfigPathname(), buf, pn.Config.DefaultFileMode)

    return err

}

func (pn *Pnode) GetConfigPathname() string {
    return path.Join(pn.Params.BasePath, PnodeConfigFilename)
}


func (pn *Pnode) SetupDirForClient(inClientID []byte) string {
    clientDir := pn.FSNameEncoding.EncodeToString(inClientID)

    pathname := path.Join(pn.Params.ClientStoragePath, clientDir)

    os.MkdirAll(pathname, pn.Config.DefaultFileMode)

    return pathname
}







func (pn *Pnode) CreateNewCommunity( inCommunityName string ) *CommunityRepo {

    pn.Config.RepoList = append(pn.Config.RepoList, CommunityRepoInfo{})

    info := &pn.Config.RepoList[len(pn.Config.RepoList)-1]
    
    {
        info.CommunityName = inCommunityName
        info.CommunityID = make([]byte, plan.CommunityIDSz)
        rand.Read(info.CommunityID)

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
            info.TimeCreated = plan.Now()
            info.MaxPeerClockDelta = 60 * 25
        }

    }

    CR := NewCommunityRepo(info, pn)

    pn.RegisterCommunityRepo(CR)

    pn.WriteConfigOut()

    return CR

}


const (
    UnlockPersonalKeyring = "/plan/SKI/local/1"
)

// StartSession implements pservice.PserviceServer
func (pn *Pnode) StartSession(
    ctx context.Context,
    inSessionReq *pservice.SessionRequest,
    ) (*pservice.SessionInfo, error) {

    communityID := plan.GetCommunityID(in.CommunityId)

    CR := pn.CRbyID[communityID]
    if CR == nil {
        return nil, plan.Errorf(nil, plan.CommunityNotFound, "communityID not found {ID:%v}", in.CommunityId)
    }
/*
    keyCrypt, err := CR.ReadMemberFile(in.ClientId, "keyring")
    if err != nil {
        return nil, plan.Error(err, plan.KeyringStoreNotFound, "keyring store not found")
    }
*/  //

    // See https://github.com/melvincarvalho/gold for TLS in Go 

    clientDir := SetupDirForClient(in.ClientId)

    ski.StartSession(ski.SessionParams{
    })

    /*
    skiSession, err := ski.StartSession( 
        *inSessionReq.SKIInvocation,
        ski.GatewayRWAccess,
        clientDir,
        nil,
    )
    if err != nil {
        return nil, err
    }

    skiSession.DispatchOp(OpArgs{
        ski.OpAcceptKeys, */


    session := NewClientSession(inSessionReq)

    // TODO security checks to prevent DoS
    pn.ActiveSessions.InsertSession(session)
    
    sessionInfo := &pservice.SessionInfo{
        SessionToken: session.AuthToken,
    }

	return sessionInfo, nil
}


// ReportStatus implements pservice.PserviceServer
func (pn *Pnode) ReportStatus(
    ctx context.Context, 
    in *pservice.StatusQuery,
    ) (*pservice.StatusReply, error) {

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








