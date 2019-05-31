
package main

import (
    //"fmt"
    "flag"
    "io/ioutil"
    "os"

    log "github.com/sirupsen/logrus"
    "time"
    //"io"
    "bufio"
    "bytes"
    "fmt"
	//"os/user"
    "path"
    //"crypto/md5"
    //"github.com/plan-systems/go-plan/ski/Providers/hive"
	//_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
    //"encoding/hex"
    "encoding/json"
    "strings"
    crand "crypto/rand" 


    "github.com/plan-systems/go-plan/repo"

    "github.com/plan-systems/go-plan/pcore"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/pdi"
    //"github.com/plan-systems/go-plan/pservice"

    "github.com/ethereum/go-ethereum/common/hexutil"

    "context"
 	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"

    //ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    //"github.com/plan-systems/go-plan/ski/Providers/filesys"


    //"github.com/denisbrodbeck/machineid"


)


func main() {

    init        := flag.Bool  ( "init",         false,      "Simulates a new installation" )
    seed        := flag.String( "seed",         "",         "Reads the given member seed pathname and reflates repo and account")
    basePath    := flag.String( "path",         "",         "Directory for all files associated with this repo" )


    flag.Parse()

    ws, err := NewWorkstation(basePath, *init)
    if err != nil {
        log.Fatal(err)
    }

    seedMember := false
    if len(*seed) > 0 {
        _, err = ws.ImportSeed(*seed)
        seedMember = true
    }
    if err != nil {
        log.Fatal(err)
    }


    {
        intr, intrCtx := plan.SetupInterruptHandler(context.Background())
        defer intr.Close()
        
        wsCtx, err := ws.Startup(intrCtx)
        if err != nil {
            log.WithError(err).Fatalf("failed to startup client-sim")
        }

        log.Infof("to stop service: kill -s SIGINT %d\n", os.Getpid())

        ws.Login(0, seedMember)

        select {
            case <- wsCtx.Done():
        }

        ws.Shutdown("client done")
    }   

}


// Workstation represents a client "install" on workstation or mobile device.
// It's distinguished by its ability to connect to a pnode and become "a dumb terminal" 
// for the given pnode.  Typically, the PLAN is on the same physical device as a pnode
// so that the file system is shared, but this is not required.
//
// For a given PLAN install on a given workstation/device:
// (a) Only one person ("user") is permitted to use it at a time.  
// (b) Any number of persons/agents can use it (like user accounts on a traditional OS)
// (c) A User has any number of communities (repos) seeded, meaning it has established an
//     account on a given pnode for a given community. 
type Workstation struct {
    flow                        plan.Flow

    BasePath                    string
    UsersPath                   string
    Info                        InstallInfo

    Seeds                       []string

    MemberSeed                  repo.MemberSeed
}


// InstallInfo is generated during client installation is considered immutable. 
type InstallInfo struct {

    InstallID                   hexutil.Bytes                   `json:"install_id"`

}

// NewWorkstation creates a new Workstation instance
func NewWorkstation(
    inBasePath *string,
    inDoInit bool,
) (*Workstation, error) {

    ws := &Workstation{

    }
/*
id, err := machineid.ProtectedID("myAppName")
  if err != nil {
    log.Fatal(err)
  }*/

    var err error

    // FUTURE: base 
    if inBasePath == nil || len(*inBasePath) == 0 {
        ws.BasePath, err = plan.UseLocalDir("workstation")
    } else {
        ws.BasePath = *inBasePath
    }
    if err != nil { return nil, err }

    ws.UsersPath = path.Join(ws.BasePath, "users")

    if err = os.MkdirAll(ws.UsersPath, plan.DefaultFileMode); err != nil {
        return nil, err
    }

    if err = ws.readConfig(inDoInit); err != nil {
        return nil, err
    }

    return ws, nil
}



func (ws *Workstation) readConfig(inFirstTime bool) error {

    pathname := path.Join(ws.BasePath, "InstallInfo.json")
 
    buf, err := ioutil.ReadFile(pathname)
    if err == nil { 
        err = json.Unmarshal(buf, &ws.Info)
    }
    if err != nil {
        if os.IsNotExist(err) && inFirstTime {
            ws.Info.InstallID = make([]byte, plan.WorkstationIDSz)
            crand.Read(ws.Info.InstallID)

            buf, err = json.MarshalIndent(&ws.Info, "", "\t")
            if err == nil {
                err = ioutil.WriteFile(pathname, buf, plan.DefaultFileMode)
            }
        } else {
            err = plan.Errorf(err, plan.ConfigFailure, "Failed to load client/workstation install info")
        }
    }

    return err
}




// Startup --
func (ws *Workstation) Startup(
    inCtx context.Context,
) (context.Context, error) {

    err := ws.flow.Startup(
        inCtx, 
        "Workstaion",
        ws.internalStartup,
        ws.internalShutdown,
    )

    return ws.flow.Ctx, err
}

// Shutdown --
func (ws *Workstation) Shutdown(inReason string) {

    ws.flow.Shutdown(inReason)

}


func (ws *Workstation) internalStartup() error {

    dirs, err := ioutil.ReadDir(ws.UsersPath)
    if err != nil {
        return err
    }

    for _, dir := range dirs {
        itemPath := dir.Name()
        if ! strings.HasPrefix(itemPath, ".") {
            ws.Seeds = append(ws.Seeds, itemPath)
        }
    }

    return nil
}



func (ws *Workstation) internalShutdown() {


}


const seedFilename = "member.seed"


// ImportSeed --
func (ws *Workstation) ImportSeed(
    inSeedPathname string,
) (*repo.MemberSeed, error) {

    buf, err := ioutil.ReadFile(inSeedPathname)
    if err != nil {
        return nil, err
    }

    seed := &repo.MemberSeed{}
    if err = seed.Unmarshal(buf); err != nil {
        return nil, err
    }

    userDir := fmt.Sprintf("%s-%s", seed.RepoSeed.SuggestedDirName, seed.MemberEpoch.FormMemberStrID())
    userDir, err = plan.CreateNewDir(ws.UsersPath, userDir)
    if err != nil {
        return nil, err
    }

    err = ioutil.WriteFile(path.Join(userDir, seedFilename), buf, plan.DefaultFileMode)
    if err != nil {
        return nil, err
    }

    return seed, nil
}


type UserSession struct {
    //flow                plan.Flow

    repoConn            *grpc.ClientConn
    MemberSeed          repo.MemberSeed
    Info                repo.GenesisSeed
    SessionInfo         *repo.SessionInfo
}



func (ws *Workstation) Login(inNum int, inSeedMember bool) error {

    seedPathname := path.Join(ws.UsersPath, ws.Seeds[inNum], seedFilename)

    buf, err := ioutil.ReadFile(seedPathname)
    if err != nil {
        return err
    }

    sess := UserSession{

    }

    if err = sess.MemberSeed.Unmarshal(buf); err != nil {
        return err
    }

 
    {
        unpacker := ski.NewUnpacker(false)

        var packingInfo ski.SignedPayload
        err = unpacker.UnpackAndVerify(sess.MemberSeed.RepoSeed.SignedGenesisSeed, &packingInfo)
        if err == nil {
            err = sess.Info.Unmarshal(packingInfo.Header)
            if err == nil {
                if ! bytes.Equal(packingInfo.Signer.PubKey, sess.Info.StorageEpoch.OriginKey.PubKey) {
                    err = plan.Errorf(nil, plan.VerifySignatureFailed, "failed to verify %v", seedPathname)
                }
            }
        }
    }

	repoCtx, _ := context.WithCancel(ws.flow.Ctx)

    //addr := ws.MemberSeed.RepoSeed.Services[0].Addr
    // TODO: StorageProvider node is tracker!?
    addr :=  ":50082"
    sess.repoConn, err = grpc.DialContext(repoCtx, addr, grpc.WithInsecure())
    if err != nil {
        log.WithError(err).Fatalf("failed to connect to pnode")
    }

    repoClient := repo.NewRepoClient(sess.repoConn)

    // TODO: move this to ImportSeed()
    if inSeedMember {
        _, err := repoClient.SeedMember(repoCtx, &sess.MemberSeed)
        if err != nil {
            return err

            // TODO close gprc conn
        }    
    }

    var header, trailer metadata.MD
    sess.SessionInfo, err = repoClient.StartMemberSession(repoCtx, 
        &repo.SessionReq{
            WorkstationID: ws.Info.InstallID,
            CommunityID: sess.Info.CommunityEpoch.CommunityID,
            MemberEpoch: sess.MemberSeed.MemberEpoch,
        },
        grpc.Header(&header), 
        grpc.Trailer(&trailer),
    )
    if err != nil {
        log.WithError(err).Fatalf("StartMemberSession() error")
    }

    reader := bufio.NewReader(os.Stdin)
    fmt.Println("Press enter for OpenChannelSession()")
    reader.ReadString('\n')

    repoCtx, err = pcore.TransferSessionToken(repoCtx, trailer)
    chSession, err := repoClient.OpenChannelSession(repoCtx, &repo.ChInvocation{
        ChannelGenesis: &pdi.ChannelEpoch{
            ChProtocol: repo.ChProtocolTalk,
            ACC: sess.Info.StorageEpoch.RootACC(),
        },
    })
    if err != nil {
        log.WithError(err).Fatalf("OpenChannelSession() error")
    }

    chMsg, err := chSession.Recv()
    if err != nil {
        log.WithError(err).Fatalf("OpenChannelSession() error")
    }

    sessID := chMsg.SessionID

    fmt.Println("Press enter for ChSessionPipe()")
    reader.ReadString('\n')

    chSessPipe, err := repoClient.ChSessionPipe(repoCtx)
    if err != nil {
        log.WithError(err).Fatalf("ChSessionPipe() error")
    }


    heartbeat := time.NewTicker(time.Second * 15).C
    newTalkie := time.NewTicker(time.Second * 5).C

    for msgID := uint32(1); true; msgID++ {
        
        select {
            case <- chSession.Context().Done():
                log.Info("chSession.Context().Done()")
            case <- chSessPipe.Context().Done():
                log.Info("chSessPipe.Context().Done()")
            case <- heartbeat:
                chSessPipe.Send(&repo.ChMsg{
                    MsgID: msgID,
                    Op: repo.ChMsgOp_HEARTBEAT,
                    SessionID: sessID,
                })
            case <- newTalkie:
                chSessPipe.Send(&repo.ChMsg{
                    MsgID: msgID,
                    Op: repo.ChMsgOp_CH_NEW_ENTRY,
                    SessionID: sessID,
                    ChEntry: &repo.ChEntryMsg{
                        EntryOp: pdi.EntryOp_POST_CONTENT,
                        Body: []byte(fmt.Sprintf("Hello world %d", msgID)),
                    },
                })
        }


    }

    return nil
}


func sleepTillDrew() {

    keepGoing := 1

    for keepGoing != 0 {
        keepGoing++
        time.Sleep(1 * time.Second)
    }

}

/*


    // Seed a new repo?
    if len(*seed) > 0 {
        buf, err := ioutil.ReadFile(*seed)
        if err != nil {
            log.Fatal(err)
        }

        seed := &repo.MemberSeed{}
        if err = seed.Unmarshal(buf); err != nil {
            log.Fatal(err)
        }

        if err = pn.SeedRepo(seed.RepoSeed); err != nil {
            log.WithError(err).Fatalf("seed failed from %v", *seed)*
        }
    }

            if *test {
                go func() {
                    buf, _ := ioutil.ReadFile("/Users/aomeara/go/src/github.com/plan-systems/go-plan/cmd/pdi-local/yoyo.plan.seed")

                    seed := &repo.MemberSeed{}
                    seed.Unmarshal(buf)

                    var workstationID [plan.CommunityIDSz]byte
                    for i := 0; i < plan.CommunityIDSz; i++ {
                        workstationID[i] = byte(i)
                    }

                    ////// TEMP
                    genesisSeed := repo.GenesisSeed{}
                    var packingInfo ski.SignedPayload
                    unpacker := ski.NewUnpacker(false)
                    err = unpacker.UnpackAndVerify(seed.RepoSeed.SignedGenesisSeed, &packingInfo)
                    if err == nil {
                        err = genesisSeed.Unmarshal(packingInfo.Header)
                    }
                    ms, _, _ := pn.StartMemberSession(context.Background(), &repo.SessionReq{
                        WorkstationID: workstationID[:],
                        CommunityID: genesisSeed.CommunityEpoch.CommunityID,
                        MemberEpoch: seed.MemberEpoch,
                    })

                    ms.GetItWorking()
                }()
            }*/



/*

    var err error
	CR.spContext, CR.spCancel = context.WithCancel(CR.flow.Ctx)

    addr := CR.State.Services[0].Addr
	CR.spClientConn, err = grpc.DialContext(CR.spContext, addr, grpc.WithInsecure())
	if err != nil {
        err = plan.Errorf(err, plan.FailedToConnectStorageProvider, "grpc.Dial() failed with addr %v", addr)
	}

    if err == nil {
	    CR.spClient = pdi.NewStorageProviderClient(CR.spClientConn)
    }
		
    if err == nil {
        var header, trailer metadata.MD
        CR.spInfo, err = CR.spClient.StartSession(
            CR.spContext, 
            &pdi.SessionReq{
                StorageEpoch: CR.GenesisSeed.StorageEpoch,
            },
            grpc.Header(&header), 
            grpc.Trailer(&trailer),
        )
        if err != nil {
            err = plan.Error(err, plan.FailedToConnectStorageProvider, "StartSession() failed")
        }

        if err == nil {
            CR.spContext, err = pcore.TransferSessionToken(CR.spContext, trailer)
            if err != nil {
                err = plan.Error(err, plan.FailedToConnectStorageProvider, "TransferSessionToken() failed")
            }
        }
    }

    if err != nil {
        CR.DisconnectFromStorage()
        return err
    }

    CR.txnScanning.Add(1)
    go CR.forwardTxnScanner()












var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

func (ms *MemberSession) GetItWorking() {

    for i := 0; i < 100 && ms.flow.IsRunning() && false; i++ {

        cheese := fmt.Sprintf("#%d: %s", i, gTestBuf)


        ms.EntriesToCommit <- &entryIP{
            Entry: chEntry{
                Info: pdi.EntryInfo{
                    EntryOp: pdi.EntryOp_POST_CONTENT,
                    ChannelID: 123,
                    AuthorMemberID: ms.MemberEpoch.MemberID,
                    AuthorMemberEpoch: ms.MemberEpoch.EpochNum },
                Body: []byte(cheese),
            },
        }
        
        time.Sleep(10 * time.Second)
    }
}

*/