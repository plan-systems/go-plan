

package main

import (

    "os"
    "path"
    "io/ioutil"
    //"strings"
    "sync"
    //"time"
    "net"
    //"encoding/hex"
    "encoding/json"
    "encoding/base64"

    log "github.com/sirupsen/logrus"



    //"github.com/tidwall/redcon"

    //"github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/pservice"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"
    //github.com/plan-systems/go-plan/ski"
    _ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

    //"github.com/plan-systems/go-plan/ski/Providers/nacl"

    // This inits in with sql, so no named import is needed
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"
    "crypto/rand"

    //"github.com/stretchr/testify/assert"
    "github.com/ethereum/go-ethereum/common/hexutil"



    //"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

    //"github.com/ethereum/go-ethereum/rlp"

    "golang.org/x/net/context"

)





const (

    // DefaultGrpcNetworkName is the default net.Listen() network layer name
    DefaultGrpcNetworkName      = "tcp"

    // DefaultGrpcNetworkAddr is the default net.Listen() local network address
    DefaultGrpcNetworkAddr      = ":50053"

    // CurrentSnodeVersion specifies the Snode version 
    CurrentSnodeVersion         = "0.1"

    // ConfigFilename is the file name the root stage config resides in
    ConfigFilename              = "storage.config.json"

)



// Snode represents an instance of a running Snode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Snode struct {
    Stores                      map[plan.CommunityID]*ds.Store

    ActiveSessions              pservice.SessionGroup

    BasePath                    string
    Config                      Config

    grpcServer                  *grpc.Server
    listener                    net.Listener
    grpcDone                    chan struct{}
    ShuttingDown                chan struct{}

    FSNameEncoding              *base64.Encoding
}


// RuntimeSettings specifies settings that are solely associated with system load, performance, and resource allocation.
type RuntimeSettings struct {

}




// Config specifies all operating parameters if a Snode (PLAN's p2p/server node)
type Config struct {
    //BasePath                    string                          `json:"base_path"`

    Name                        string                          `json:"node_name"`
    NodeID                      hexutil.Bytes                   `json:"node_id"`

    StorageConfigs              []ds.StorageConfig              `json:"storage_configs"`

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

/*
func (config *Config) ReadFromFile(inPathname string) *plan.Perr {
    buf, err := ioutil.ReadFile(inPathname)
    if err != nil {
        return plan.Errorf(err, plan.ConfigNotRead, "error reading Snode config file %v", inPathname)
    }

    err = json.Unmarshal(buf, config)
    if err != nil {
        return plan.Errorf(err, plan.ConfigNotRead, "error unmarshalling Snode config file %v", inPathname)
    }

    return err
}

*/


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
        ShuttingDown: make(chan struct{}),
        grpcDone: make(chan struct{}),
        Stores: make(map[plan.CommunityID]*ds.Store),
    }

    sn.ActiveSessions.Init()

    var err error

    if inBasePath != nil && len(*inBasePath) > 0 {
        sn.BasePath = *inBasePath
    } else {
        sn.BasePath, err = plan.UseLocalDir("")
    }

    if err == nil {
        err = os.MkdirAll(sn.BasePath, plan.DefaultFileMode)
        if err != nil {
            err = plan.Errorf(err, plan.FailedToAccessPath, "failed to setup Snode.BasePath %v", sn.BasePath)
        }
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
            log.WithError(err).Infof("Failed to load %s", pathname)
        }
    }

    return err
}

// WriteConfig writes out the node config file based on sn.BasePath
func (sn *Snode) WriteConfig() error {

    pathname := path.Join(sn.BasePath, ConfigFilename)

    err := sn.Config.WriteToFile(pathname)
    if err != nil {
        return plan.Errorf(err, plan.FailedToAccessPath, "Failed to write Snode config %s", pathname)
    }

    return nil
}


// CreateNewStore creates a new data store and adds it to this nodes list of stores (and updates the config on disk)
func (sn *Snode) CreateNewStore(
    inConfig *ds.StorageConfig,
    inDeposits []*pdi.Transfer,
) error {
    
    var err error

    // Prep the db path
    {
        if path.IsAbs(inConfig.HomePath) {

            if _, err := os.Stat(inConfig.HomePath); ! os.IsNotExist(err) {
                return plan.Errorf(nil, plan.FailedToAccessPath, "for safety, the path '%s' must not already exist", inConfig.HomePath)
            }

            err = os.MkdirAll(inConfig.HomePath, sn.Config.DefaultFileMode)

        } else {

            pathname := path.Join(sn.BasePath, inConfig.HomePath)
            err = os.Mkdir(pathname, sn.Config.DefaultFileMode)
        }

        if err != nil {
            return plan.Errorf(err, plan.FailedToAccessPath, "could not create the new storage path %s", inConfig.HomePath)
        }
    }

    St := ds.NewStore(inConfig, sn.BasePath)
    if err = St.Startup(true); err != nil {
        return err
    }

    if err = St.DepositTransfers(inDeposits); err != nil {
        return err
    }
    
    sn.Config.StorageConfigs = append(sn.Config.StorageConfigs, *inConfig)

    if err = sn.WriteConfig(); err != nil {
        return err
    }

    // Register the new store so it will be shutdown properly
    sn.registerStore(St)

    return nil
}

// Startup should be called after ReadConfig() and any desired calls to CreateStore()
func (sn *Snode) Startup() error {

    for i := range sn.Config.StorageConfigs {
        info := &sn.Config.StorageConfigs[i]
        
        St := ds.NewStore(info, sn.BasePath)

        sn.registerStore(St)
    }

    for _, St := range sn.Stores {
        err := St.Startup(false)
        if err != nil {
            return err
        }
    }

    return nil
}



func (sn *Snode) registerStore(St *ds.Store) {

    communityID := plan.GetCommunityID(St.Config.Epoch.CommunityID)
    //var communityID plan.CommunityID
    //copy(communityID[:], CS.Info.CommunityID)

    // TODO: add mutex!
    sn.Stores[communityID] = St

}




// StartServer is used like main()
func (sn *Snode) StartServer() {
    var err error

    log.Infof("Starting StorageProvider service on %v, %v", sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    sn.listener, err = net.Listen(sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    if err != nil {
        log.WithError(err).Error("net.Listen()")
    }

    // TODO: turn off compression since we're dealing w/ encrypted data
    sn.grpcServer = grpc.NewServer()
    pdi.RegisterStorageProviderServer(sn.grpcServer, sn)
    
    // Register reflection service on gRPC server.
    reflection.Register(sn.grpcServer)
    go func() {

        if err := sn.grpcServer.Serve(sn.listener); err != nil {
            log.WithError(err).Warn("grpcServer.Serve()")
        }
        
        sn.listener.Close()
        sn.listener = nil

        close(sn.grpcDone)
    }()

}


// Shutdown performs a shutdown and cleanup
func (sn *Snode) Shutdown() {
    log.Info("Shutting down...")

    if sn.grpcServer != nil {
        sn.grpcServer.GracefulStop()

        log.Info("Waiting on server")
        <- sn.grpcDone
    }

    group := sync.WaitGroup{}
    group.Add(len(sn.Stores))

    log.Debug("Shutting down stores...")
    for _, St := range sn.Stores {
        go St.Shutdown(&group)
    }

    group.Wait()

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
func (sn *Snode) StartSession(ctx context.Context, in *pservice.SessionRequest) (*pdi.StorageSession, error) {
    cID := plan.GetCommunityID(in.CommunityId)

    St := sn.Stores[cID]
    if St == nil {
        return nil, plan.Errorf(nil, plan.CommunityNotFound, "community not found {ID:%v}", in.CommunityId)
    }


    /******* gRPC Server Notes **********
        1) An endpoint w/ a stream response sends an io.EOF once the handler proc returns (even if streamer stays open)
    */

    // TODO security checks to prevent DoS
    session :=  sn.ActiveSessions.NewSession(ctx)
    session.Cookie = St
    /*
    go func() {
        for i := 0; i < 10; i++ {
            tryDesc := fmt.Sprintf("try-%v", i)
            log.Print("sending: ", tryDesc)

            inOutlet.Send(&pservice.Msg{
                Label: tryDesc,
            })

            time.Sleep(5 * time.Second)   
        }
    }()

    time.Sleep(60 * time.Second)   */

    msg := &pdi.StorageSession{
        TxnEncoderInvocation: St.TxnDecoder.TxnEncoderInvocation(),
    }
	return msg, nil
}

// Query -- see service StorageProvider in pdi.proto
func (sn *Snode) Query(inQuery *pdi.TxnQuery, inOutlet pdi.StorageProvider_QueryServer) error {
    session, err := sn.ActiveSessions.FetchSession(inOutlet.Context())
    if err != nil {
        return err
    }

    // TODO: use sync.Pool to reduce allocs
    job := &ds.QueryJob{
        TxnQuery:  inQuery,
        Outlet:    inOutlet,
        OnComplete: make(chan error),
    }

    St := session.Cookie.(*ds.Store)
    St.QueryInbox <- job

    err = <-job.OnComplete
    return err
}

var txnAwaitingCommit = pdi.TxnMetaInfo{
    TxnStatus: pdi.TxnStatus_AWAITING_COMMIT,
}

// CommitTxn -- see service StorageProvider in pdi.proto
func (sn *Snode) CommitTxn(inTxn *pdi.ReadiedTxn, inOutlet pdi.StorageProvider_CommitTxnServer) error {
    session, err := sn.ActiveSessions.FetchSession(inOutlet.Context())
    if err != nil {
        return err
    }
    job := &ds.CommitJob{
        ReadiedTxn: inTxn,
        Outlet:     inOutlet,
        OnComplete: make(chan error),
    }

    job.Outlet.Send(&txnAwaitingCommit)

    St := session.Cookie.(*ds.Store)
    St.CommitInbox <- job

    err = <-job.OnComplete
    return err
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
        
        CS := NewDatastore(CSInfo, sn)

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




