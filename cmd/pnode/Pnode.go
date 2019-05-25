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
    "strings"
    crand "crypto/rand" 
    //"fmt"
    

 	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
    //"google.golang.org/grpc/metadata"

    "github.com/ethereum/go-ethereum/common/hexutil"

    //"github.com/plan-systems/go-plan/pdi"
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

    DefaultFileMode             os.FileMode                     `json:"default_file_mode"`

    GrpcNetworkName             string                          `json:"grpc_network"`
    GrpcNetworkAddr             string                          `json:"grpc_addr"`

    Version                     int32                           `json:"version"`

}



// ApplyDefaults sets std fields and values
func (config *PnodeConfig) ApplyDefaults() {

    config.DefaultFileMode = plan.DefaultFileMode
    config.GrpcNetworkName = "tcp"
    config.GrpcNetworkAddr = ":50081"
    config.Version = 1

}


// Pnode wraps one or more communities replicated to a local dir.
type Pnode struct {
    flow                        plan.Flow

    reposMutex                  sync.RWMutex
    repos                       map[plan.CommunityID]*repo.CommunityRepo
    
    activeSessions              pcore.SessionGroup

    BasePath                    string
    ReposPath                   string
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
        pn.BasePath, err = plan.UseLocalDir("pnode")
    } else {
        pn.BasePath = *inBasePath
    }
    if err != nil { return nil, err }

    pn.ReposPath = path.Join(pn.BasePath, "repos")

    if err = os.MkdirAll(pn.ReposPath, plan.DefaultFileMode); err != nil {
        return nil, err
    }

    if err = pn.readConfig(inDoInit); err != nil {
        return nil, err
    }

    return pn, nil
}





// readConfig uses BasePath to read in the node's config file
func (pn *Pnode) readConfig(inFirstTime bool) error {

    pathname := path.Join(pn.BasePath, configFilename)
 
    buf, err := ioutil.ReadFile(pathname)
    if err == nil { 
        err = json.Unmarshal(buf, &pn.Config)
    }
    if err != nil {
        if os.IsNotExist(err) && inFirstTime {
            pn.Config.ApplyDefaults()
            pn.Config.NodeID = make([]byte, plan.CommunityIDSz)
            crand.Read(pn.Config.NodeID)

            err = pn.writeConfig()
        } else {
            err = plan.Errorf(err, plan.ConfigFailure, "Failed to load pnode config")
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

// TODO: test w/ sym links
    repoDirs, err := ioutil.ReadDir(pn.ReposPath)
    if err != nil {
        return err
    }

    for _, repoDir := range repoDirs {
        repoPath := repoDir.Name()
        if ! strings.HasPrefix(repoPath, ".") {
            repoPath := path.Join(pn.ReposPath, repoPath)
            CR, err := repo.NewCommunityRepo(repoPath, nil)
            if err != nil {
                return err
            }

            pn.registerRepo(CR)
        }
    }

    for _, CR := range pn.repos {
        err = CR.Startup(pn.flow.Ctx)
        if err != nil {
            break
        }
    }

    if err == nil {
        err = pn.startServer()
    }

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

    if pn.grpcServer != nil {
        pn.flow.Log.Info("Stopping Repo grpc service")
        pn.grpcServer.GracefulStop()

        _, _ = <- pn.grpcDone
        pn.flow.Log.Debug("Repo stopped")
    }

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


// seedRepo adds a new repo (if it doesn't already exist)
func (pn *Pnode) seedRepo(
    inSeed *repo.RepoSeed,
) error {

    // In the unlikely event that pn.Shutdown() is called while this is all happening, 
    //    prevent the rug from being snatched out from under us.
    pn.flow.ShutdownComplete.Add(1)
    defer pn.flow.ShutdownComplete.Done()

    if ! pn.flow.IsRunning() {
        return plan.Error(nil, plan.AssertFailed, "pnode must be running to seed a new repo")
    }

    // Only proceed if the dir doesn't exist
    repoPath, err := plan.CreateNewDir(pn.ReposPath, inSeed.SuggestedDirName)
    if err != nil { return err }

    // When we pass the seed, it means create from scratch
    CR, err := repo.NewCommunityRepo(repoPath, inSeed)
    if err != nil { return err }

    if err == nil {
        err = CR.Startup(pn.flow.Ctx)
    }

    if err == nil {
        err = pn.writeConfig()
    }

    if err == nil {
        if pn.flow.IsRunning() {
            pn.registerRepo(CR)
        } else {
            CR.Shutdown("creation complete")
        }
    }

    return err
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

    err = ms.CheckStatus()
    if err != nil {
        return nil, err
    }

    return ms, nil
}



func (pn *Pnode) registerRepo(CR *repo.CommunityRepo) {
   
    communityID := plan.GetCommunityID(CR.GenesisSeed.StorageEpoch.CommunityID)

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


func (pn *Pnode) startServer() error {

    pn.grpcDone = make(chan struct{})

    pn.flow.Log.Infof("starting Repo service on %v %v", pn.Config.GrpcNetworkName, pn.Config.GrpcNetworkAddr)
    listener, err := net.Listen(pn.Config.GrpcNetworkName, pn.Config.GrpcNetworkAddr)
    if err != nil {
        return err
    }

    pn.grpcServer = grpc.NewServer()
    repo.RegisterRepoServer(pn.grpcServer, pn)
    
    // Register reflection service on gRPC server.
    reflection.Register(pn.grpcServer)
    go func() {

        if err := pn.grpcServer.Serve(listener); err != nil {
            pn.flow.LogErr(err, "grpc Serve() failed")
        }
        
        listener.Close()

        close(pn.grpcDone)
    }()

    return nil
}

/*****************************************************
**
**
**
** rpc service Repo
**
**
**
**/

// SeedMember -- see service Repo in repo.proto.
func (pn *Pnode) SeedMember(
    ctx context.Context, 
    in *repo.MemberSeed,
) (*plan.Status, error) {

    err := pn.seedRepo(in.RepoSeed)
    if err != nil {
        return nil, err
    }

    // Set up the member sub dir and write the intital KeyTome
    // For now we can skip this b/c the KeyTime is already known to be local
    {
        // TODO
    }

    return &plan.Status{}, nil
}

// StartMemberSession -- see service Repo in repo.proto.
func (pn *Pnode) StartMemberSession(
    ctx context.Context, 
    in *repo.SessionReq,
) (*repo.SessionInfo, error) {

    CR := pn.fetchRepo(in.CommunityID)
    if CR == nil {
        return nil, plan.Error(nil, plan.CommunityNotFound, "community not found")
    }

    ms, err := CR.StartMemberSession(in)
    if err != nil {
        return nil, err
    }

    sess := pn.activeSessions.NewSession(ctx)
    sess.Cookie = ms

    info := &repo.SessionInfo{
    }

	return info, nil
}

// OpenChannelSession -- see service Repo in repo.proto.
func (pn *Pnode) OpenChannelSession(
    inInvocation *repo.ChInvocation, 
    io repo.Repo_OpenChannelSessionServer,
) error {
    ms, err := pn.fetchMemberSession(io.Context())
    if err != nil {
        return err
    }

    _, err = ms.OpenChannelSession(inInvocation, io)
    if err != nil {
        return err
    }

    ctx := io.Context()
    <- ctx.Done()

    return nil
}

// ChSessionPipe -- see service Repo in repo.proto.
func (pn *Pnode) ChSessionPipe(in repo.Repo_ChSessionPipeServer) error {
    ms, err := pn.fetchMemberSession(in.Context())
    if err != nil {
        return err
    }

    return ms.ManageChSessionPipe(in)
}

