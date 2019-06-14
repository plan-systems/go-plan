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
    config.GrpcNetworkAddr = ":50082"
    config.Version = 1

}


// Pnode wraps one or more communities replicated to a local dir.
type Pnode struct {
    plan.Context

    reposMutex                  sync.RWMutex
    repos                       map[plan.CommunityID]*repo.CommunityRepo
    
    activeSessions              plan.SessionGroup

    BasePath                    string
    ReposPath                   string
    Config                      PnodeConfig

    grpcServer                  *grpc.Server
    listener                    net.Listener
}


// NewPnode creates a new Pnode
func NewPnode(
    inBasePath *string,
    inDoInit bool,
) (*Pnode, error) {

    pn := &Pnode{
        repos: make(map[plan.CommunityID]*repo.CommunityRepo),
        activeSessions: plan.NewSessionGroup(),
    }

    pn.SetLogLabel("pnode")

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
        err = CR.Startup(pn.Ctx)
        if err != nil {
            break
        }
    }

    //
    //
    //
    // grpc service
    //
    if err == nil {
        pn.Infof(0, "starting service on %v %v", pn.Config.GrpcNetworkName, pn.Config.GrpcNetworkAddr)
        listener, err := net.Listen(pn.Config.GrpcNetworkName, pn.Config.GrpcNetworkAddr)
        if err != nil {
            return err
        }

        pn.grpcServer = grpc.NewServer()
        repo.RegisterRepoServer(pn.grpcServer, pn)
        
        // Register reflection service on gRPC server.
        reflection.Register(pn.grpcServer)
        pn.CtxGo(func(*plan.Context) {
            if err := pn.grpcServer.Serve(listener); err != nil {
                pn.Error("grpc server error: ", err)
            }
            pn.CtxInitiateStop("grpc server stopped")
            listener.Close()
        })
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
            CR.CtxStop(pn.CtxStopReason())
            reposRunning.Done()
        }()
    }
    pn.reposMutex.RUnlock()

    if pn.grpcServer != nil {
        pn.Info(1, "stopping grpc service")
        pn.grpcServer.GracefulStop()
    }

    reposRunning.Wait()

}


// Startup -- see pcore.Flow.Startup
func (pn *Pnode) Startup(
    inCtx context.Context,
) error {

    err := pn.CtxStart(
        inCtx, 
        pn.internalStartup,
        pn.internalShutdown,
        nil,
    )

    return err
}

// seedRepo adds a new repo (if it doesn't already exist)
func (pn *Pnode) seedRepo(
    inSeed *repo.RepoSeed,
) error {

    var CR *repo.CommunityRepo

    {
        genesis, err := inSeed.ExtractAndVerifyGenesisSeed()
        if err !=  nil {
            return err
        }

        // If the repo is already seed, nothing further required
        CR = pn.fetchRepo(genesis.StorageEpoch.CommunityID)
        if CR != nil {
            return nil
        }
    }

    if ! pn.CtxRunning() {
        return plan.Error(nil, plan.AssertFailed, "pnode must be running to seed a new repo")
    }

    // Only proceed if the dir doesn't exist
    // TODO: change dir name in the event of a name collision.
    repoPath, err := plan.CreateNewDir(pn.ReposPath, inSeed.SuggestedDirName)
    if err != nil { return err }

    // In the unlikely event that pn.Shutdown() is called while this is all happening, 
    //    prevent the rug from being snatched out from under us.
    hold := make(chan struct{})
    defer func() {
        hold <- struct{}{}
    }()
    pn.CtxGo(func(*plan.Context) {
        <- hold
    })

    // When we pass the seed, it means create from scratch
    if err == nil {
        CR, err = repo.NewCommunityRepo(repoPath, inSeed)
    }

    if err == nil {
        err = CR.Startup(pn.Ctx)
    }

    if err == nil {
        err = pn.writeConfig()
    }

    if err == nil {
        pn.Info(0, "seeding new repo at ", repoPath)
        pn.registerRepo(CR)
    } else {
        CR.CtxStop("seed failed")

        // TODO: clean up
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

    err = ms.CtxStatus()
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

/*****************************************************
**
**
**
** rpc service Repo
**
**
**
**/

// SeedRepo -- see service Repo in repo.proto.
func (pn *Pnode) SeedRepo(
    ctx context.Context, 
    inRepoSeed *repo.RepoSeed,
) (*plan.Status, error) {

    err := pn.seedRepo(inRepoSeed)
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

// OpenMemberSession -- see service Repo in repo.proto.
func (pn *Pnode) OpenMemberSession(
    inSessReq *repo.MemberSessionReq, 
    inMsgOutlet repo.Repo_OpenMemberSessionServer,
) error {

    CR := pn.fetchRepo(inSessReq.CommunityID)
    if CR == nil {
        return plan.Error(nil, plan.CommunityNotFound, "community not found")
    }

    ms, err := CR.OpenMemberSession(inSessReq, inMsgOutlet)
    if err != nil {
        return err
    }

//
// TODO: remove active session when the ms goes away

    // Because this a streaming call, headers and trailers won't ever arrive.  
    // Instead, the ms passes it manually and so we have to add it here.
    sess := pn.activeSessions.NewSession(inMsgOutlet.Context(), ms.SessionToken)
    sess.Cookie = ms

    // Should this be waiting on ms.flow.StopComplete instead?
    select {
        case <- inMsgOutlet.Context().Done():
    }

	return nil
}

// StartChannelSession -- see service Repo in repo.proto.
func (pn *Pnode) StartChannelSession(
    ctx context.Context, 
    inInvocation *repo.ChInvocation, 
) (*repo.ChSessionInfo, error) {
    ms, err := pn.fetchMemberSession(ctx)
    if err != nil {
        return nil, err
    }

    chSession, err := ms.StartChannelSession(inInvocation)
    if err != nil {
        return nil, err
    }

    info := &repo.ChSessionInfo{
        SessID: uint32(chSession.ChSessID),
    }

    return info, nil
}

// OpenMsgPipe -- see service Repo in repo.proto.
func (pn *Pnode) OpenMsgPipe(inMsgInlet repo.Repo_OpenMsgPipeServer) error {
    ms, err := pn.fetchMemberSession(inMsgInlet.Context())
    if err != nil {
        return err
    }

    return ms.OpenMsgPipe(inMsgInlet)
}