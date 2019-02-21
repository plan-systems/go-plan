

package main

import (

    "os"
    "path"
    "io/ioutil"
    //"strings"
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





const (

    // DefaultGrpcNetworkName is the default net.Listen() network layer name
    DefaultGrpcNetworkName      = "tcp"

    // DefaultGrpcNetworkAddr is the default net.Listen() local network address
    DefaultGrpcNetworkAddr      = ":50053"

    // CurrentSnodeVersion specifies the Snode version 
    CurrentSnodeVersion         = "0.1"

    // ConfigFilename is the file name the root stage config resides in
    ConfigFilename              = "storage-config.json"

)



// Snode represents an instance of a running Snode daemon.  Multiple disk-independent instances
//     can be instantiated and offer service in parallel, this is not typical operation. Rather,
//     one instance runs and hosts service for one or more communities.
type Snode struct {
    Stores                      map[plan.CommunityID]*ds.Store
    
    ActiveSessions              pservice.SessionGroup

    ctx                         context.Context

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
    inCtx context.Context,
    inBasePath *string,
) (*Snode, error) {

    sn := &Snode{
        ctx: inCtx,
        grpcDone: make(chan struct{}),
        Stores: make(map[plan.CommunityID]*ds.Store),
    }

    sn.ActiveSessions.Init()

    var err error

    if inBasePath != nil && len(*inBasePath) > 0 {
        sn.BasePath = *inBasePath
    } else {
        sn.BasePath, err = plan.UseLocalDir("Stores")
    }

    if err == nil {
        err = os.MkdirAll(sn.BasePath, plan.DefaultFileMode)
        if err != nil {
            err = plan.Errorf(err, plan.FailedToAccessPath, "failed to setup Snode.BasePath %v", sn.BasePath)
        }
    }

    if err != nil {
        return nil, err
    }

    return sn, nil
}

// ReadConfig uses sn.BasePath to read in the node config file
func (sn *Snode) ReadConfig(inInit bool) error {

    pathname := path.Join(sn.BasePath, ConfigFilename)
 
    err := sn.Config.ReadFromFile(pathname)
    if err != nil {
        if os.IsNotExist(err) {
            if inInit {
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
    
    stConfig := &ds.StorageConfig{
        HomePath: path.Join("datastore", plan.MakeFSFriendly(inEpoch.CommunityName, inEpoch.CommunityID[:2])),
        ImplName: inImplName,
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

        // Write out the community genesis file
        {
            buf, jerr := json.MarshalIndent(&inEpoch, "", "\t")
            if jerr != nil {
                return jerr
            }

            epPathname := path.Join(stPathname, pdi.GenesisEpochFilename)
            if err = ioutil.WriteFile(epPathname, buf, sn.Config.DefaultFileMode); err != nil {
                return plan.Errorf(err, plan.FailedToAccessPath, "could not create %s", epPathname)
            }
        }
    }

    St := ds.NewStore(stConfig, sn.BasePath)
    if err = St.Startup(sn.ctx, true); err != nil {
        return err
    }

    if err = St.DepositTransfers(inDeposits); err != nil {
        return err
    }
    
    sn.Config.StorageConfigs = append(sn.Config.StorageConfigs, *stConfig)

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
        err := St.Startup(sn.ctx, false)
        if err != nil {
            return err
        }
    }

    return nil
}


func (sn *Snode) registerStore(St *ds.Store) {

    communityID := plan.GetCommunityID(St.Epoch.CommunityID)
    //var communityID plan.CommunityID
    //copy(communityID[:], CS.Info.CommunityID)

    // TODO: add mutex!
    sn.Stores[communityID] = St

}

// StartServer is used like main()
func (sn *Snode) StartServer() error {
    var err error

    log.Infof("Starting StorageProvider service on %v, %v", sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    sn.listener, err = net.Listen(sn.Config.GrpcNetworkName, sn.Config.GrpcNetworkAddr)
    if err != nil {
        return plan.Error(err, plan.NetworkNotReady, "failed to start StorageProvider")
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

    return nil
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

// StartSession -- see service StorageProvider in pdi.proto
func (sn *Snode) StartSession(ctx context.Context, in *pservice.SessionRequest) (*pdi.StorageInfo, error) {
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

    msg := &pdi.StorageInfo{
        EncodingDesc: St.TxnDecoder.EncodingDesc(),
    }
	return msg, nil
}

// Query -- see service StorageProvider in pdi.proto
func (sn *Snode) Query(inQuery *pdi.TxnQuery, inOutlet pdi.StorageProvider_QueryServer) error {
    session, err := sn.ActiveSessions.FetchSession(inOutlet.Context())
    if err != nil {
        return err
    }

    St, _ := session.Cookie.(*ds.Store)
    if St != nil {

        job := ds.QueryJob{
            TxnQuery:  inQuery,
            Outlet:    inOutlet,
            OnComplete: make(chan error),
        }
        
        St.DoQueryJob(job)

        err = <-job.OnComplete
    }

    return err
}

// SendTxns -- see service StorageProvider in pdi.proto
func (sn *Snode) SendTxns(inTxnBatch *pdi.TxnBatch, inOutlet pdi.StorageProvider_SendTxnsServer) error {
    session, err := sn.ActiveSessions.FetchSession(inOutlet.Context())
    if err != nil {
        return err
    }

    St, _ := session.Cookie.(*ds.Store)
    if St != nil {
        
        job := ds.SendJob{
            UTIDs:     inTxnBatch.UTIDs,
            Outlet:    inOutlet,
            OnComplete: make(chan error),
        }

        St.DoSendJob(job)

        err = <-job.OnComplete
    }

    return err
}


// CommitTxn -- see service StorageProvider in pdi.proto
func (sn *Snode) CommitTxn(inRawTxn *pdi.RawTxn, inOutlet pdi.StorageProvider_CommitTxnServer) error {
    session, err := sn.ActiveSessions.FetchSession(inOutlet.Context())
    if err != nil {
        return err
    }

    St, _ := session.Cookie.(*ds.Store)
    if St != nil {

        job := ds.CommitJob{
            Outlet:     inOutlet,
            Txn:        pdi.DecodedTxn{
                RawTxn: inRawTxn.Bytes,
            },
            OnComplete: make(chan error),
        }

        St.CommitInbox <- job

        err = <-job.OnComplete
    }

    return err
}
