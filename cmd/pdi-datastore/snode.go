

package main

import (

    //"fmt"
    "os"
    "path"
    "io/ioutil"
    //"strings"
    //"sync"
    //"time"
    "net"
    //"encoding/hex"
    "encoding/json"
    "encoding/base64"

    log "github.com/sirupsen/logrus"

    ds "github.com/ipfs/go-datastore"
    badger "github.com/ipfs/go-ds-badger"

    //"github.com/tidwall/redcon"

    "github.com/plan-systems/go-plan/plan"
    //github.com/plan-systems/go-plan/ski"
    _ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

    //"github.com/plan-systems/go-plan/ski/Providers/nacl"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"
    "crypto/rand"

    //"github.com/stretchr/testify/assert"


    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/pservice"

    //"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

    //"github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/common/hexutil"

    "golang.org/x/net/context"

)





const (

    // DefaultGrpcNetworkName is the default net.Listen() network layer name
    DefaultGrpcNetworkName      = "tcp"

    // DefaultGrpcNetworkAddr is the default net.Listen() local network address
    DefaultGrpcNetworkAddr      = ":50051"

    // CurrentSnodeVersion specifies the Snode version 
    CurrentSnodeVersion         = "0.1"

    // ConfigFilename is the file name the root stage config resides in
    ConfigFilename              = "storage.config.json"

)



// CStorage wraps a PLAN community UUID and a datastore
type CStorage struct {
    CommunityID                 plan.CommunityID
    
    Snode                       *Snode

    Info                        *CStorageInfo

    ds                          ds.Datastore

    AbsPath                     string

    msgOutbox                   chan *pservice.Msg
    msgInbox                    chan *pservice.Msg

    DefaultFileMode             os.FileMode

}



func newCStorage(
    inInfo *CStorageInfo,
    inParent *Snode,
) *CStorage {

    CS := &CStorage{
        Snode: inParent,
        Info: inInfo,
        DefaultFileMode: inParent.Config.DefaultFileMode,
        msgOutbox: make(chan *pservice.Msg, 16),
        msgInbox: make(chan *pservice.Msg, 16),
    }

    if path.IsAbs(CS.Info.HomePath) {
        CS.AbsPath = CS.Info.HomePath
    } else {
        CS.AbsPath = path.Clean(path.Join(inParent.BasePath, CS.Info.HomePath))
    }

    return CS
}


// OnServiceStarting should be once CStorage is preprared and ready to invoke the underlying implementation.
func (CS *CStorage) OnServiceStarting() error {

    logE := log.WithFields(log.Fields{ 
        "impl": CS.Info.ImplName,
        "path": CS.AbsPath,
    })
    logE.Info( "OnServiceStarting()" )

    var err error

    switch CS.Info.ImplName {
        case "badger":
            CS.ds, err = badger.NewDatastore(CS.AbsPath, nil)
        default:
            err = plan.Errorf(nil, plan.StorageImplNotFound, "storage implementation for '%s' not found", CS.Info.ImplName)
    }

    if err != nil {
        logE := logE.WithError(err)
        logE.Warn("OnServiceStarting() ERROR")
    }

    return err
}






// Snode represents an instance of a running Snode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Snode struct {
    Stores                      map[plan.CommunityID]*CStorage

    ActiveSessions              pservice.SessionGroup

    BasePath                    string
    Config                      Config

    FSNameEncoding              *base64.Encoding
}


// RuntimeSettings specifies settings that are solely associated with system load, performance, and resource allocation.
type RuntimeSettings struct {

}


// CStorageInfo contains core info about a db/store
type CStorageInfo struct {
    CommunityName           string                  `json:"community_name"`
    CommunityID             hexutil.Bytes           `json:"community_id"`
    HomePath                string                  `json:"home_path"`
    TimeCreated             plan.Time               `json:"time_created"`  
    ImplName                string                  `json:"impl_name"`
    ImplParams              map[string]string       `json:"impl_params"`
}




// Config specifies all operating parameters if a Snode (PLAN's p2p/server node)
type Config struct {
    //BasePath                    string                          `json:"base_path"`

    Name                        string                          `json:"node_name"`
    NodeID                      hexutil.Bytes                   `json:"node_id"`

    Stores                      []CStorageInfo                  `json:"node_info"`

    RuntimeSettings             RuntimeSettings                 `json:"runtime_settings"`

    DefaultFileMode             os.FileMode                     `json:"default_file_mode"`

    GrpcNetworkName             string                          `json:"grpc_network"`
    GrpcNetworkAddr             string                          `json:"grpc_addr"`

    Version                     int32                           `json:"version"`

}




// ApplyDefaults sets std fields and values
func (config *Config) ApplyDefaults() {

    config.DefaultFileMode = plan.DefaultFileMode
    config.GrpcNetworkName = DefaultGrpcNetworkName
    config.GrpcNetworkAddr = DefaultGrpcNetworkAddr
    config.Version = 1

}


// ReadFromFile attempts to load node config info from the given pathname.
func (config *Config) ReadFromFile(inPathname string) error {
    buf, err := ioutil.ReadFile(inPathname)
    if err != nil {
        return err
    }

    err = json.Unmarshal(buf, config)

    return err
}


// WriteToFile attempts to save node config info to the given pathname.
func (config *Config) WriteToFile(inPathname string) error {

    buf, err := json.MarshalIndent(&config, "", "\t")
    if err != nil {
        return err
    }

    err = ioutil.WriteFile(inPathname, buf, config.DefaultFileMode)

    return err

}





// NewSnode creates and initializes a new Snode instance
func NewSnode(inBasePath *string) *Snode {
    sn := &Snode{
        FSNameEncoding: base64.RawURLEncoding,
    }

    sn.Stores = map[plan.CommunityID]*CStorage{}
    sn.ActiveSessions.Init()

    var err error

    if inBasePath != nil && len(*inBasePath) > 0 {
        sn.BasePath = *inBasePath
    } else {
        sn.BasePath, err = plan.UseLocalDir("")
    }

    if err == nil {
        err = os.MkdirAll(sn.BasePath, plan.DefaultFileMode)
    }

    if err != nil {
        log.Fatal(err)
    }

    return sn
}


// ReadConfig uses sn.BasePath to read in the node config file
func (sn *Snode) ReadConfig(inInit bool) error {

    pathname := path.Join(sn.BasePath, ConfigFilename)

    err := sn.Config.ReadFromFile(pathname)
    if err != nil {
        if os.IsNotExist(err) {
            log.WithError(err).Warn("storage node config not found")
            if inInit {
                sn.Config.ApplyDefaults()
                sn.Config.NodeID = make([]byte, plan.CommunityIDSz)
                rand.Read(sn.Config.NodeID)

                err = sn.WriteConfig()
            }
        } else {
            log.WithError(err).Info("Failed to load %s", pathname)
        }
    }

    return err
}

// WriteConfig writes out the node config file based on sn.BasePath
func (sn *Snode) WriteConfig() error {

    pathname := path.Join(sn.BasePath, ConfigFilename)

    err := sn.Config.WriteToFile(pathname)
    if err != nil {
        log.WithError(err).Info("Failed to write config %s", pathname)
    }

    return err
}


// CreateNewStore creates a new data store and adds it to this nodes list of stores (and updates the config on disk)
func (sn *Snode) CreateNewStore(CSInfo *CStorageInfo) error {

    sn.Config.Stores = append(sn.Config.Stores, *CSInfo)

    return nil
}

// Startup should be called after ReadConfig() and any desired calls to CreateStore()
func (sn *Snode) Startup() error {

    for i := range sn.Config.Stores {
        CSInfo := &sn.Config.Stores[i]
        
        CS := newCStorage(CSInfo, sn)

        sn.registerCStore(CS)
    }

    for _, CS := range sn.Stores {
        err := CS.OnServiceStarting()
        if err != nil {
            return err
        }
    }

    return nil
}



func (sn *Snode) registerCStore(CS *CStorage) {

    communityID := plan.GetCommunityID(CS.Info.CommunityID)
    //var communityID plan.CommunityID
    //copy(communityID[:], CS.Info.CommunityID)

    // TODO: add mutex!
    sn.Stores[communityID] = CS

}




// Run is used like main()
func (sn *Snode) Run() {

    {
        lis, err := net.Listen(sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
        if err != nil {
            log.Fatalf( "failed to listen: %v", err )
        }
        grpcServer := grpc.NewServer()
        pdi.RegisterStorageProviderServer(grpcServer, sn)
        
        // Register reflection service on gRPC server.
        reflection.Register(grpcServer)
        if err := grpcServer.Serve( lis ); err != nil {
            log.Fatalf( "failed to serve: %v", err )
        }
    }
}






/*

var storageMsgPool = sync.Pool{
    New: func() interface{} {
        return new(StorageMsg)
    },
}

// RecycleStorageMsg effectively deallocates the item and makes it available for reuse
func RecycleStorageMsg(inMsg *StorageMsg) {
    for _, txn := range inMsg.Txns {
        txn.Body = nil  // TODO: recycle plan.Blocks too
    }
    storageMsgPool.Put(inMsg)
}

// NewStorageMsg allocates a new StorageMsg
func NewStorageMsg() *StorageMsg {

    msg := storageMsgPool.Get().(*StorageMsg)
    if msg == nil {
        msg = &StorageMsg{}
    } else {
        msg.Txns = msg.Txns[:0]
        msg.AlertCode = 0
        msg.AlertMsg = ""
    }

    return msg
}

// NewStorageAlert creates a new storage msg with the given alert params
func NewStorageAlert(
    inAlertCode AlertCode, 
    inAlertMsg string,
    ) *StorageMsg {

    msg := NewStorageMsg()
    msg.AlertCode = inAlertCode
    msg.AlertMsg = inAlertMsg

    return msg 

}



// SnodeID identifies a particular Snode running as part of a PLAN community
type SnodeID                [32]byte

func (pnID SnodeID) MarshalJSON() ( []byte, error ) {
    bytesNeeded := base64.RawURLEncoding.EncodedLen( len(pnID) ) + 2
    outText := make( []byte, bytesNeeded, bytesNeeded )
    outText[0] = '"'
    base64.RawURLEncoding.Encode( outText[1:bytesNeeded-1], pnID[:] )
    outText[bytesNeeded-1] = '"'

    return outText, nil
}

func (pnID SnodeID) UnmarshalJSON( inText []byte ) error {
    _, err := base64.RawURLEncoding.Decode( pnID[:], inText )

    return err
}
*/

// StartSession -- see service StorageProvider in pdi.proto
func (sn *Snode) StartSession(in *pservice.SessionRequest, inOutlet pdi.StorageProvider_StartSessionServer) error {

    cID := plan.GetCommunityID(in.CommunityId)

    DS := sn.Stores[cID]
    if DS == nil {
        return plan.Errorf(nil, plan.CommunityNotFound, "community not found {ID:%v}", in.CommunityId)
    }


    // TODO security checks to prevent DoS
    session :=  sn.ActiveSessions.NewSession(inOutlet.Context(), in)

    inOutlet.Send(&pservice.Msg{
        Label: "SessionTokenKey",
        Body: &plan.Block{
            Label: pservice.SessionTokenKey,
            Content: []byte(session.AuthToken),
        },
    })
    
    // TODO insert delay

	return nil
}

// PostResponse -- see service StorageProvider in pdi.proto
func (sn *Snode) PostResponse(ctx context.Context, inMsg *pservice.Msg) (*plan.Status, error) {

    session, err := sn.ActiveSessions.FetchSession(ctx)
    if err != nil {
        return nil, err
    }

	return &plan.Status{
        Code: 0,
        Msg: "Hello token:"  + session.AuthToken,
    }, nil
}

// Query -- see service StorageProvider in pdi.proto
func (sn *Snode) Query(inQuery *pdi.QueryTxns, inOutlet pdi.StorageProvider_QueryServer) error {
    return nil
}

// SubmitTxn -- see service StorageProvider in pdi.proto
func (sn *Snode) SubmitTxn(ctx context.Context, inTxn *pdi.TxnCandidate) (*pdi.StagedTxn, error) {
    return nil, nil
}

// CommitTxn -- see service StorageProvider in pdi.proto
func (sn *Snode) CommitTxn(inTxn *pdi.StagedTxn, inOutlet pdi.StorageProvider_CommitTxnServer) error {
    return nil
}


/*
func (sn *Snode) InitOnDisk(initWithStorageType string) error {

    sn.Config.SnodeID = make([]byte, plan.CommunityIDSz)
    rand.Read(sn.Config.SnodeID)

    var datastore *ds.Datastore
    var err error
    
    switch sn.Config.ImplName {
        case "badger":
            datastore, err = badger.NewDatastore(path, nil)
    }

    sn.Config.ImplName = initWithStorageType

    return sn.WriteConfigOut()
}


func (sn *Snode) LoadConfigIn() error {

    buf, err := ioutil.ReadFile(sn.GetConfigPathname())
    if err != nil {
        return err
    }

    err = json.Unmarshal(buf, &sn.Config)
    if err != nil {
        return err
    }

    for i := range sn.Config.Stores {
        CSInfo := &sn.Config.Stores[i]
        
        CS := NewCStorage(CSInfo, sn)

        sn.RegisterCStore(CS)
    }

    return err
}
*/


/*

func (sn *Snode) SetupDirForClient(inClientID []byte) string {
    clientDir := sn.FSNameEncoding.EncodeToString(inClientID)

    pathname := path.Join(sn.Params.ClientStoragePath, clientDir)

    os.MkdirAll(pathname, sn.Config.DefaultFileMode)

    return pathname
}
*/




/*

func (sn *Snode) CreateNewCommunity( inCommunityName string ) *CommunityRepo {

    sn.Config.RepoList = append(sn.Config.RepoList, CommunityRepoInfo{})

    info := &sn.Config.RepoList[len(sn.Config.RepoList)-1]
    
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

    sn.RegisterCommunityRepo(CR)

    sn.WriteConfigOut()

    return CR

}



*/



