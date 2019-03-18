package main

import (
    "os"
    "path"
    "io/ioutil"
    //"strings"
    "sync"
    //"time"
    //"sort"
    //"encoding/hex"
    "encoding/json"
    "context"
    "net"
    crand "crypto/rand" 
    //"fmt"
    

 	"google.golang.org/grpc"
    //"google.golang.org/grpc/metadata"

    "github.com/ethereum/go-ethereum/common/hexutil"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/pcore"
    "github.com/plan-systems/go-plan/repo"

    //"github.com/dgraph-io/badger"

)

const (
    configFilename              = "PnodeConfig.json"
)

// PnodeConfig specifies all operating parameters if a Snode (PLAN's p2p/server node)
type PnodeConfig struct {

    Name                        string                          `json:"node_name"`
    NodeID                      hexutil.Bytes                   `json:"node_id"`

    RepoConfigs                 []repo.Config                   `json:"repo_configs"`

    DefaultFileMode             os.FileMode                     `json:"default_file_mode"`

    GrpcNetworkName             string                          `json:"grpc_network"`
    GrpcNetworkAddr             string                          `json:"grpc_addr"`

    Version                     int32                           `json:"version"`

}



// ApplyDefaults sets std fields and values
func (config *PnodeConfig) ApplyDefaults() {

    config.DefaultFileMode = plan.DefaultFileMode
    config.GrpcNetworkName = "tcp"
    config.GrpcNetworkAddr = ":50053"
    config.Version = 1

}


// Pnode wraps one or more communities replicated to a local dir.
type Pnode struct {
    flow                        plan.Flow

    reposMutex                  sync.RWMutex
    repos                       map[plan.CommunityID]*repo.CommunityRepo
    
    activeSessions              pcore.SessionGroup

    BasePath                    string
    Config                      PnodeConfig

    grpcServer                  *grpc.Server
    listener                    net.Listener
    grpcDone                    chan struct{}
}


// NewPnode creates a new Pnode
func NewPnode(
    inBasePath *string,
    inDoInit bool,
) (*Pnode, error) {

    pn := &Pnode{
        repos: make(map[plan.CommunityID]*repo.CommunityRepo),
        activeSessions: pcore.NewSessionGroup(),
    }

    var err error

    if inBasePath == nil || len(*inBasePath) == 0 {
        pn.BasePath, err = plan.UseLocalDir("Repos")
    } else {
        pn.BasePath = *inBasePath
    }
    if err != nil { return nil, err }

    if err = os.MkdirAll(pn.BasePath, plan.DefaultFileMode); err != nil {
        return nil, err
    }

    if err = pn.readConfig(inDoInit); err != nil {
        return nil, err
    }

    return pn, nil
}





// readConfig uses BasePath to read in the node's config file
func (pn *Pnode) readConfig(inFirstTome bool) error {

    pathname := path.Join(pn.BasePath, configFilename)
 
    buf, err := ioutil.ReadFile(pathname)
    if err == nil { 
        err = json.Unmarshal(buf, &pn.Config)
    }
    if err != nil {
        if os.IsNotExist(err) {
            if inFirstTome {
                pn.Config.ApplyDefaults()
                pn.Config.NodeID = make([]byte, plan.CommunityIDSz)
                crand.Read(pn.Config.NodeID)

                err = pn.writeConfig()
            }
        } else {
            err = plan.Errorf(err, plan.ConfigFailure, "Failed to load node config")
        }
    }

    return err
}

// writeConfig writes out the node config file based on BasePath
func (pn *Pnode) writeConfig() error {

    buf, err := json.MarshalIndent(&pn.Config, "", "\t")
    if err == nil {
        pathname := path.Join(pn.BasePath, configFilename)

        err = ioutil.WriteFile(pathname, buf, pn.Config.DefaultFileMode)
    }

    if err != nil {
        return plan.Errorf(err, plan.FailedToAccessPath, "Failed to write node config")
    }

    return nil
}



func (pn *Pnode) internalStartup() error {


    for i := range pn.Config.RepoConfigs {

        CR := repo.NewCommunityRepo(
            &pn.Config.RepoConfigs[i],
            pn.BasePath,
        )
        
        pn.registerRepo(CR)
    }

    var err error
    for _, CR := range pn.repos {
        err = CR.Startup(pn.flow.Ctx)
        if err != nil {
            break
        }
    }

    /*
    if err == nil {
        err = pn.startServer()
    }
    */

    return err
}



func (pn *Pnode) internalShutdown() {

    // Shutdown the Stores FIRST so that all we have to do is wait on the server to stop.
    reposRunning := &sync.WaitGroup{}

    pn.reposMutex.RLock()
    reposRunning.Add(len(pn.repos))
    
    for _, v := range pn.repos {
        CR := v
        go func() {
            CR.Shutdown(pn.flow.ShutdownReason)
            reposRunning.Done()
        }()
    }
    pn.reposMutex.RUnlock()


/*
    if pn.grpcServer != nil {
        log.Info("initiating grpc graceful stop")
        pn.grpcServer.GracefulStop()

        _, _ = <- pn.grpcDone
        log.Info("grpc server done")
    }*/

    reposRunning.Wait()

}


// Startup -- see pcore.Flow.Startup
func (pn *Pnode) Startup(
    inCtx context.Context,
) (context.Context, error) {

    err := pn.flow.Startup(
        inCtx, 
        "Pnode",
        pn.internalStartup,
        pn.internalShutdown,
    )

    return pn.flow.Ctx, err
}

// Shutdown -- see pcore.Flow.Startup
func (pn *Pnode) Shutdown(inReason string) {

    pn.flow.Shutdown(inReason)

}




// InflateRepo adds a new repo (if it doesn't already exist)
func (pn *Pnode) InflateRepo(
    inSeed *pdi.RepoSeed,
) error {

    if pn.flow.IsRunning() {
        //pn.registerRepo(CR)
    }

    config := &repo.Config{
        HomePath: plan.MakeFSFriendly(inSeed.CommunityEpoch.CommunityName, inSeed.StorageEpoch.CommunityID[:2]),
        StorageEpoch: *inSeed.StorageEpoch,
    }

    err := plan.CreateNewDir(pn.BasePath, config.HomePath)
    if err != nil { return err }

    CR := repo.NewCommunityRepo(
        config, 
        pn.BasePath,
    )

    if err == nil {
        err = CR.SetupRepoHome(inSeed)
    }

    pn.flow.ShutdownComplete.Add(1)

    if err == nil {
        err = CR.Startup(context.Background())
    }

    if err == nil {

        pn.Config.RepoConfigs = append(pn.Config.RepoConfigs, *config)

        err = pn.writeConfig()
    }

    if err == nil {
        if pn.flow.IsRunning() {
            pn.registerRepo(CR)
        } else {
            CR.Shutdown("creation complete")
        }
    }

    pn.flow.ShutdownComplete.Done()

    return err
}


// StartMemberSession -- see repo.proto.
func (pn *Pnode) StartMemberSession(
    ctx context.Context, 
    in *repo.SessionReq,
) (*repo.MemberSession, *repo.SessionInfo, error) {

    CR := pn.fetchRepo(in.CommunityID)
    if CR == nil {
        return nil, nil, plan.Error(nil, plan.CommunityNotFound, "community not found")
    }

    ms, err := CR.StartMemberSession(in)
    if err != nil {
        return nil, nil, err
    }

    sess := pn.activeSessions.NewSession(ctx)
    sess.Cookie = ms

    info := &repo.SessionInfo{
    }

	return ms, info, nil
}

// InvokeChannel -- see repo.proto.
func (pn *Pnode) InvokeChannel(ctx context.Context, in *repo.ChInvocation) (*repo.ChStatus, error) {
    _, err := pn.fetchMemberSession(ctx)
    if err != nil {
        return nil, err
    }
    //pn.activeSessions.FetchSession


    return nil, nil
}



// ChSessionPipe -- see repo.proto.
func (pn *Pnode) ChSessionPipe(in repo.Repo_ChSessionPipeServer) error {
    _, err := pn.fetchMemberSession(in.Context())
    if err != nil {
        return err
    }

    return nil
}



func (pn *Pnode) fetchMemberSession(ctx context.Context) (*repo.MemberSession, error) {
    session, err := pn.activeSessions.FetchSession(ctx)
    if err != nil {
        return nil, err
    }

    ms, _ := session.Cookie.(*repo.MemberSession)
    if ms == nil {
        return nil, plan.Errorf(nil, plan.AssertFailed, "internal type assertion err")
    }

    err = ms.Flow.CheckStatus()
    if err != nil {
        return nil, err
    }

    return ms, nil
}




func (pn *Pnode) registerRepo(CR *repo.CommunityRepo) {
   
    communityID := plan.GetCommunityID(CR.Config.StorageEpoch.CommunityID)

    pn.reposMutex.Lock()
    pn.repos[communityID] = CR
    pn.reposMutex.Unlock()
}


func (pn *Pnode) fetchRepo(inID []byte) *repo.CommunityRepo {

    communityID := plan.GetCommunityID(inID)

    pn.reposMutex.RLock()
    CR := pn.repos[communityID]
    pn.reposMutex.RUnlock()

    return CR

}



