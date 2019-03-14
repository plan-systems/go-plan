

package main

import (

    "os"
    "path"
    "io/ioutil"
    "fmt"
    "sync"
    //"time"
    "net"
    "crypto/rand"
    //"encoding/hex"
    "encoding/json"

    log "github.com/sirupsen/logrus"

    //"github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/pservice"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"
    _ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"    

    "github.com/ethereum/go-ethereum/common/hexutil"

    "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

)


// GenesisParams is entered by humans
type GenesisParams struct {
    CommunityName           string                  `json:"community_name"`
    CommunityID             hexutil.Bytes           `json:"community_id"`
    //GenesisAddr             hexutil.Bytes           `json:"genesis_addr"`
}





const (

    // DefaultGrpcNetworkName is the default net.Listen() network layer name
    DefaultGrpcNetworkName      = "tcp"

    // DefaultGrpcNetworkAddr is the default net.Listen() local network address
    DefaultGrpcNetworkAddr      = ":50053"

    // CurrentSnodeVersion specifies the Snode version 
    CurrentSnodeVersion         = "0.1"

    // ConfigFilename is the file name the root stage config resides in
    ConfigFilename              = "config.json"

)



// Snode represents an instance of a running Snode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Snode struct {
    flow                        plan.Flow

    Stores                      map[plan.CommunityID]*ds.Store
    
    ActiveSessions              pservice.SessionGroup

    BasePath                    string
    Config                      Config

    grpcServer                  *grpc.Server
    listener                    net.Listener
    grpcDone                    chan struct{}
}



// Config specifies all operating parameters if a Snode (PLAN's p2p/server node)
type Config struct {
    //BasePath                    string                          `json:"base_path"`

    Name                        string                          `json:"node_name"`
    NodeID                      hexutil.Bytes                   `json:"node_id"`

    StorageConfigs              []ds.StorageConfig              `json:"storage_configs"`

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
func NewSnode(
    inBasePath *string,
    inDoInit bool,
) (*Snode, error) {


    sn := &Snode{
        Stores: make(map[plan.CommunityID]*ds.Store),
    }
    
    sn.ActiveSessions.Init()

    var err error

    if inBasePath == nil || len(*inBasePath) == 0 {
        sn.BasePath, err = plan.UseLocalDir("Stores")
    } else {
        sn.BasePath = *inBasePath
    }

    if err == nil {
        err = os.MkdirAll(sn.BasePath, plan.DefaultFileMode)
        if err != nil {
            err = plan.Errorf(err, plan.FailedToAccessPath, "failed to setup Snode.BasePath %v", sn.BasePath)
        }
    }

    if err == nil {
        err = sn.ReadConfig(inDoInit)
        if err != nil {
            log.WithError(err).Fatalf("sn.ReadConfig failed")
        }
    }

    if err != nil {
        return nil, err
    }

    return sn, nil
}

// Startup -- see plan.Flow.Startup()
func (sn *Snode) Startup(inCtx context.Context) (context.Context, error) {

    err := sn.flow.Startup(
        inCtx,
        fmt.Sprintf("StorageProvider %x", sn.Config.NodeID[:2]),
        sn.onInternalStartup,
        sn.onInternalShutdown,
    )

    return sn.flow.Ctx, err
}

// Shutdown -- see plan.Flow.Shutdown()
func (sn *Snode) Shutdown(
    inReason string,
) {

    sn.flow.Shutdown(inReason)
}



func (sn *Snode) onInternalStartup() error {

    var err error


    for i := range sn.Config.StorageConfigs {
        info := &sn.Config.StorageConfigs[i]
        
        St := ds.NewStore(info, sn.BasePath)

        sn.registerStore(St)
    }

    for _, St := range sn.Stores {
        err := St.Startup(sn.flow.Ctx, false)
        if err != nil {
            return err
        }
    }

    if err == nil {
        err = sn.startServer()
    }

    return err
}



func (sn *Snode) onInternalShutdown() {

    // Shutdown the Stores FIRST so that all we have to do is wait on the server to stop.
    storesRunning := &sync.WaitGroup{}

    storesRunning.Add(len(sn.Stores))
    
    for _, v := range sn.Stores {
        St := v
        go func() {
            St.Shutdown(sn.flow.ShutdownReason)
            storesRunning.Done()
        }()
    }

    if sn.grpcServer != nil {
        log.Info("initiating grpc graceful stop")
        sn.grpcServer.GracefulStop()

        _, _ = <- sn.grpcDone
        log.Info("grpc server done")
    }

    storesRunning.Wait()

}



// ReadConfig uses sn.BasePath to read in the node's config file
func (sn *Snode) ReadConfig(inFirstTome bool) error {

    pathname := path.Join(sn.BasePath, ConfigFilename)
 
    err := sn.Config.ReadFromFile(pathname)
    if err != nil {
        if os.IsNotExist(err) {
            if inFirstTome {
                sn.Config.ApplyDefaults()
                sn.Config.NodeID = make([]byte, plan.CommunityIDSz)
                rand.Read(sn.Config.NodeID)

                err = sn.WriteConfig()
            }
        } else {
            err = plan.Errorf(err, plan.ConfigFailure, "Failed to load %s", pathname)
        }
    }

    return err
}

// WriteConfig writes out the node config file based on sn.BasePath
func (sn *Snode) WriteConfig() error {

    pathname := path.Join(sn.BasePath, ConfigFilename)

    err := sn.Config.WriteToFile(pathname)
    if err != nil {
        return plan.Errorf(err, plan.FailedToAccessPath, "Failed to write storage node config %s", pathname)
    }

    return nil
}

// CreateNewStore creates a new data store and adds it to this nodes list of stores (and updates the config on disk)
func (sn *Snode) CreateNewStore(
    inImplName string,
    inDeposits []*pdi.Transfer,
    inEpoch pdi.StorageEpoch,
) error {
    
    if sn.flow.IsRunning() {
        return plan.Error(nil, plan.AssertFailed, "can't create store while running")
    }

    stConfig := &ds.StorageConfig{
        HomePath: path.Join("datastore", plan.MakeFSFriendly(inEpoch.Name, inEpoch.CommunityID[:2])),
        ImplName: inImplName,
        StorageEpoch: inEpoch,
    }

    var err error

    // Prep the db path & write out the community genesis in fo
    {
        var stPathname string

        if path.IsAbs(stConfig.HomePath) {
            stPathname = stConfig.HomePath
        } else {
            stPathname = path.Join(sn.BasePath, stConfig.HomePath)
        }

        if _, serr := os.Stat(stPathname); ! os.IsNotExist(serr) {
            return plan.Errorf(nil, plan.FailedToAccessPath, "for safety, the path '%s' must not already exist", stPathname)
        }

        err = os.MkdirAll(stPathname, sn.Config.DefaultFileMode)
        if err != nil {
            return plan.Errorf(err, plan.FailedToAccessPath, "failed to create storage path %s", stPathname)
        }
/*
        // Write out the community genesis file
        {
            buf, jerr := json.MarshalIndent(&inEpoch, "", "\t")
            if jerr != nil {
                return jerr
            }

            epPathname := path.Join(stPathname, pdi.StorageEpochFilename)
            if err = ioutil.WriteFile(epPathname, buf, sn.Config.DefaultFileMode); err != nil {
                return plan.Errorf(err, plan.FailedToAccessPath, "could not create %s", epPathname)
            }
        }*/
    }

    St := ds.NewStore(
        stConfig, 
        sn.BasePath,
    )
    if err = St.Startup(context.Background(), true); err != nil {
        return err
    }

    if err = St.DepositTransfers(inDeposits); err != nil {
        return err
    }
    
    sn.Config.StorageConfigs = append(sn.Config.StorageConfigs, *stConfig)

    if err = sn.WriteConfig(); err != nil {
        return err
    }

    St.Shutdown("new store creation complete")

    return nil
}


func (sn *Snode) registerStore(St *ds.Store) {

    communityID := plan.GetCommunityID(St.Config.StorageEpoch.CommunityID)
    //var communityID plan.CommunityID
    //copy(communityID[:], CS.Info.CommunityID)

    // TODO: add mutex!
    sn.Stores[communityID] = St

}


func (sn *Snode) startServer() error {

    sn.grpcDone = make(chan struct{})

    log.Infof("starting StorageProvider service on %v %v", sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    listener, err := net.Listen(sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    if err != nil {
        return plan.Error(err, plan.NetworkNotReady, "failed to start StorageProvider")
    }

    // TODO: turn off compression since we're dealing w/ encrypted data
    sn.grpcServer = grpc.NewServer()
    pdi.RegisterStorageProviderServer(sn.grpcServer, sn)
    
    // Register reflection service on gRPC server.
    reflection.Register(sn.grpcServer)
    go func() {

        if err := sn.grpcServer.Serve(listener); err != nil {
            log.WithError(err).Warn("grpcServer.Serve()")
        }
        
        listener.Close()

        close(sn.grpcDone)
    }()

    return nil
}

// StartSession -- see service StorageProvider in pdi.proto
func (sn *Snode) StartSession(ctx context.Context, in *pdi.SessionReq) (*pdi.StorageInfo, error) {
    if in.StorageEpoch == nil {
        return nil, plan.Errorf(nil, plan.ParamMissing, "missing StorageEpoch")
    }

    cID := plan.GetCommunityID(in.StorageEpoch.CommunityID)

    St := sn.Stores[cID]
    if St == nil {
        return nil, plan.Errorf(nil, plan.CommunityNotFound, "community not found {ID:%v}", in.StorageEpoch.CommunityID)
    }

    /******* gRPC Server Notes **********
        1) An endpoint w/ a stream response sends an io.EOF once the handler proc returns (even if streamer stays open)
    */

    // TODO security checks to prevent DoS
    session :=  sn.ActiveSessions.NewSession(ctx)
    session.Cookie = St

    msg := &pdi.StorageInfo{
    }

	return msg, nil
}

// FetchSessionStore uses the metadata in the given session to recover the session info and associated Store
func (sn *Snode) FetchSessionStore(ctx context.Context) (*ds.Store, error) {
    session, err := sn.ActiveSessions.FetchSession(ctx)
    if err != nil {
        return nil, err
    }

    St, _ := session.Cookie.(*ds.Store)
    if St == nil {
        return nil, plan.Errorf(nil, plan.AssertFailed, "internal type assertion err")
    }

    err = St.CheckStatus()
    if err != nil {
        return nil, err
    }

    return St, nil
}

// Scan -- see service StorageProvider in pdi.proto
func (sn *Snode) Scan(inScanPB *pdi.TxnScan, inOutlet pdi.StorageProvider_ScanServer) error {
    St, err := sn.FetchSessionStore(inOutlet.Context())
    if err != nil {
        return err
    }

    job := ds.ScanJob{
        TxnScan:   inScanPB,
        Outlet:    inOutlet,
        OnComplete: make(chan error),
    }
    
    St.DoScanJob(job)

    err = <-job.OnComplete
    return err
}

// SendTxns -- see service StorageProvider in pdi.proto
func (sn *Snode) SendTxns(inTxnList *pdi.TxnList, inOutlet pdi.StorageProvider_SendTxnsServer) error {
    St, err := sn.FetchSessionStore(inOutlet.Context())
    if err != nil {
        return err
    }
        
    job := ds.SendJob{
        UTIDs:     inTxnList.UTIDs,
        Outlet:    inOutlet,
        OnComplete: make(chan error),
    }

    St.DoSendJob(job)

    err = <-job.OnComplete
    return err
}

// CommitTxn -- see service StorageProvider in pdi.proto
func (sn *Snode) CommitTxn(ctx context.Context, inRawTxn *pdi.RawTxn) (*plan.Status, error) {
    St, err := sn.FetchSessionStore(ctx)
    if err != nil {
        return nil, err
    }

    err = St.DoCommitJob(ds.CommitJob{
        Txn: pdi.DecodedTxn{
            RawTxn: inRawTxn.Bytes,
        },
    })

    if err != nil {
        return nil, err
    }

    return &plan.Status{}, nil
}
