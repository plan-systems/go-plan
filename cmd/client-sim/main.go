
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
    "sync"
    "sync/atomic"
    "path"
    //"crypto/md5"
    "github.com/plan-systems/go-plan/ski/Providers/hive"
	//_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
    //"encoding/hex"
    "encoding/json"
    "strings"
    crand "crypto/rand" 


    "github.com/plan-systems/go-plan/repo"

    "github.com/plan-systems/go-plan/client"
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
    flag.Set("logtostderr", "true")
    flag.Set("v", "1")

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

        err = ws.Login(0, seedMember)
        if err != nil {
            log.WithError(err).Fatalf("client-sim fail")
        }

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



type msgCallback struct {
    MsgID   uint32
    F       func(inMsg *repo.Msg)
}

type msgResonder struct {
    callbacks []msgCallback
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


type msgItem struct {
    msg *repo.Msg
    entryBody []byte
    onCommitComplete OnCommitComplete
}

type msgResponder struct {
    clientSess          *ClientSess
    chSessID            repo.ChSessID
    responders          map[uint32]*msgItem
    respondersMutex     sync.Mutex
    nextMsgID           uint32
}

func (resp *msgResponder) initResponder(clientSess *ClientSess) {
    resp.responders = map[uint32]*msgItem{}
    resp.clientSess = clientSess
    resp.nextMsgID  = 1
}


func (resp *msgResponder) newMsg(op repo.MsgOp) *repo.Msg {

    msg := &repo.Msg{
        ID: atomic.AddUint32(&resp.nextMsgID, 1),
        Op: op,
        ChSessID: uint32(resp.chSessID),
    }

    return msg
}


func (resp *msgResponder) putResponder(inItem *msgItem) {
    resp.respondersMutex.Lock()
    resp.responders[inItem.msg.ID] = inItem
    resp.respondersMutex.Unlock()
}

func (resp *msgResponder) fetchResponder(inMsgID uint32, inPop bool) *msgItem {
    resp.respondersMutex.Lock()
    item := resp.responders[inMsgID]
    if item != nil && inPop {
        delete(resp.responders, inMsgID)
    }
    resp.respondersMutex.Unlock()

    return item
}

func (resp *msgResponder) handleCommon(msg *repo.Msg) {

    switch msg.Op {
                        
        case repo.MsgOp_CH_NEW_ENTRY_READY:
            if item := resp.fetchResponder(msg.ID, false); item != nil {
                item.msg = msg
                resp.clientSess.entriesToCommit <- item
            } else {
                resp.clientSess.Warnf("got CH_NEW_ENTRY_READY but msg ID %d not found", msg.ID)
            }

        case repo.MsgOp_COMMIT_COMPLETE:
            if item := resp.fetchResponder(msg.ID, true); item != nil {
                if item.onCommitComplete != nil {
                    var err error
                    if len(msg.BUF0) > 0 {
                        err = plan.Error(nil, plan.FailedToCommitTxns, string(msg.BUF0))
                    }
                    go item.onCommitComplete(msg.EntryInfo, msg.EntryState, err)
                }
            } else {
                resp.clientSess.Warnf("got MsgOp_COMMIT_COMPLETE but msg ID %d not found", msg.ID)
            }
    }
}




type chSess struct {

    ChID                plan.ChID
    msgInbox            chan *repo.Msg
    isOpen              bool

    msgResponder
}


type ClientSess struct {
    plan.Logger

    flow                plan.Flow

    ws                  *Workstation

    MemberCrypto        client.MemberCrypto

    MemberSeed          repo.MemberSeed
    Info                repo.GenesisSeed
    repoClient          repo.RepoClient

    entriesToCommit     chan *msgItem
            
    msgOutbox           chan *repo.Msg
    //msInbbox           chan *repo.Msg

    msgInlet            repo.Repo_OpenMemberSessionClient
    msgOutlet           repo.Repo_OpenMsgPipeClient

    sessToken           []byte

    chSessions          map[repo.ChSessID]*chSess
    chSessionsMutex     sync.RWMutex

    msgResponder
}



func (sess *ClientSess) Startup(
    inCtx context.Context,
) (context.Context, error) {

    sess.SetLogLabel("ClientSession")

    err := sess.flow.Startup(
        inCtx, 
        sess.GetLogLabel(),
        sess.internalStartup,
        sess.internalShutdown,
    )

    return sess.flow.Ctx, err
}


func (sess *ClientSess) internalStartup() error {

    sess.MemberCrypto.CommunityEpoch = *sess.Info.CommunityEpoch
    sess.MemberCrypto.StorageEpoch   = *sess.Info.StorageEpoch

    keyDir, err := hive.GetSharedKeyDir()
    if err != nil { return err }

    SKI, err := hive.StartSession(
        keyDir,
        sess.MemberSeed.MemberEpoch.FormMemberStrID(),
        []byte("password"),
    )
    if err != nil { return err }

    err = sess.MemberCrypto.StartSession(SKI, *sess.MemberSeed.MemberEpoch)
    if err != nil { return err }


    //
    //
    //
    go func() {

        for item := range sess.entriesToCommit {
            txnSet, err := sess.MemberCrypto.EncryptAndEncodeEntry(item.msg.EntryInfo, item.entryBody)
            if err != nil {
                sess.Warn("failed to encypt/encode entry: ", err)

                // TODO: save txns? to failed to send log/db?
            } else {
                msg := item.msg
                msg.Op = repo.MsgOp_COMMIT_TXNS
                msg.EntryInfo = nil
                msg.EntryState = nil
                msg.BUF0 = nil
                N := len(txnSet.Segs)
                msg.ITEMS = make([][]byte, N)
                for i := 0; i < N; i++ {
                    msg.ITEMS[i] = txnSet.Segs[i].RawTxn
                }
                sess.msgOutbox <- msg
            }
        }

        sess.MemberCrypto.EndSession(sess.flow.ShutdownReason)

        sess.flow.ShutdownComplete.Done()
    }()


    //
    //
    //
    go func() {

        for msg := range sess.msgOutbox {

            //sess.Infof(1, "sess.msgOutlet -> (sess %d, ID %d, %s)", msg.ChSessID, msg.ID, repo.MsgOp_name[int32(msg.Op)])

            err := sess.msgOutlet.Send(msg)
            if err != nil {
                sess.Warn("error sending msg: ", err)

                // TODO: save msg to failed to send log/db?
            }
        }

        sess.MemberCrypto.EndSession(sess.flow.ShutdownReason)

        sess.flow.ShutdownComplete.Done()
    }()

    return nil
}

func (sess *ClientSess) internalShutdown() {

    // First end all chSessions
    sess.chSessionsMutex.RLock()
    for _, cs := range sess.chSessions {
        cs.CloseSession()
    }
    sess.chSessionsMutex.RUnlock()


    if sess.msgOutbox != nil {
        close(sess.msgOutbox)
    }
}

func (sess *ClientSess) detachChSess(cs *chSess) {

    sess.chSessionsMutex.Lock()
    delete(sess.chSessions, cs.chSessID)
    sess.chSessionsMutex.Unlock()

}



func (sess *ClientSess) connectToRepo(inAddr string) error {

    repoConn, err := grpc.DialContext(sess.flow.Ctx, inAddr, grpc.WithInsecure())
    if err != nil {
        return err
    }

    sess.repoClient = repo.NewRepoClient(repoConn)

    return nil
}

func (sess *ClientSess) disconnectFromRepo() {

	
}


func (sess *ClientSess) openMemberSession() error {

    var (
        header metadata.MD
        err error
    )

    sess.msgInlet, err = sess.repoClient.OpenMemberSession(sess.flow.Ctx, 
        &repo.MemberSessionReq{
            WorkstationID: sess.ws.Info.InstallID,
            CommunityID: sess.Info.CommunityEpoch.CommunityID,
            MemberEpoch: sess.MemberSeed.MemberEpoch,
        },
        grpc.Header(&header), 
    )
    if err != nil {
        return err
    }

    msg, err := sess.msgInlet.Recv()
    if err != nil {
        return err
    }
    if msg.Op != repo.MsgOp_MEMBER_SESSION_READY || msg.ChSessID != 0 {
        return plan.Error(nil, plan.AssertFailed, "did not get valid MsgOp_MEMBER_SESSION_READY ")
    }

    sess.sessToken = msg.BUF0
 
    // Since OpenMemberSession() uses a stream responder, the trailer is never set, so we use this guy as the sesh token.
    sess.flow.Ctx = plan.ApplyTokenOutgoingContext(sess.flow.Ctx, sess.sessToken)

    sess.msgOutlet, err = sess.repoClient.OpenMsgPipe(sess.flow.Ctx)
    if err != nil {
        return err
    }

    // Send the community keyring!
    {
        msg := &repo.Msg{
            Op: repo.MsgOp_ADD_COMMUNITY_KEYS,
        }

        // TODO: come up w/ better key idea
        msg.BUF0, err = sess.MemberCrypto.ExportCommunityKeyring(sess.sessToken)
        if err != nil {
            return err
        }

        sess.msgOutbox <- msg
    }

    go func() {
        for sess.flow.IsRunning() {
            msg, err := sess.msgInlet.Recv()
            if err != nil {
                sess.flow.FilterFault(err)
            } else {
    
                //sess.Infof(1, "(sess %d, ID %d, %s) <- sess.msgInlet", msg.ChSessID, msg.ID, repo.MsgOp_name[int32(msg.Op)])

                if msg.ChSessID == 0 {
                    sess.handleCommon(msg)
                } else {
                    sess.chSessionsMutex.RLock()
                    cs := sess.chSessions[repo.ChSessID(msg.ChSessID)]
                    sess.chSessionsMutex.RUnlock()
                    if cs == nil {
                        sess.Warnf("received unrecognized ch session ID: %d", msg.ChSessID)
                    } else {
                        if cs.isOpen {
                            cs.msgInbox <- msg
                        }
                    }
                }
            }
        }
    }()

    return nil
}



func (sess *ClientSess) openChannel(
    inChID plan.ChID,
) (*chSess, error) {

    sessInfo, err := sess.repoClient.StartChannelSession(sess.flow.Ctx,
        &repo.ChInvocation{
            ChID: inChID,
            Mode: repo.ChSessionMode_REPLAY_FROM_T0,
        })

    if err != nil {
        return nil, err
    }

    cs := &chSess{
        ChID: inChID,
        msgInbox: make(chan *repo.Msg, 1),
        isOpen: true,
    }
    cs.initResponder(sess)
    cs.chSessID = repo.ChSessID(sessInfo.SessID)

    sess.chSessionsMutex.Lock()
    if sess.chSessions[cs.chSessID] != nil {
        err = plan.Error(nil, plan.AssertFailed, "ch session ID already in use")
    }
    sess.chSessions[cs.chSessID] = cs
    sess.chSessionsMutex.Unlock()

    if err != nil {
        return nil, err
    }

    go func() {

        for { 
            msg := <- cs.msgInbox
            if msg == nil {
                cs.clientSess.detachChSess(cs)          // If we're here, it's bc the channel is closing
                break
            }

            switch msg.Op {
                case repo.MsgOp_CH_ENTRY:
                    fmt.Print(msg.BUF0)
                case repo.MsgOp_CLOSE_CH_SESSION:
                    cs.CloseSession()
                default:
                    cs.handleCommon(msg)
            }
        }
    }()

    return cs, nil
}



func (cs *chSess) CloseSession() {
    if cs.isOpen {
        cs.isOpen = false
        close(cs.msgInbox)
        cs.clientSess.msgOutbox <- &repo.Msg{
            Op: repo.MsgOp_CLOSE_CH_SESSION,
            ChSessID: uint32(cs.chSessID),
        }
    }
}


// OnCommitComplete is a callback when an entry's txn(s) have been merged or rejected by the repo.
type OnCommitComplete func(
    inEntryInfo *pdi.EntryInfo,
    inEntryState *repo.EntryState,
    inErr error,
)

// OnOpenComplete is callback when a new ch session is now open and available for access.
type OnOpenComplete func(
    chSess *chSess,
    inErr error,
)

func (cs *chSess) PostContent(
    inBody []byte,
    onCommitComplete OnCommitComplete,
) {

    msg := cs.clientSess.newMsg(repo.MsgOp_CH_NEW_ENTRY_REQ)
    msg.ChSessID = uint32(cs.chSessID)
    msg.EntryInfo = &pdi.EntryInfo{
        EntryOp: pdi.EntryOp_POST_CONTENT,
    }

    cs.putResponder(&msgItem{
        msg: msg,
        entryBody: inBody,
        onCommitComplete: onCommitComplete,
    })

    cs.clientSess.msgOutbox <- msg

}


func (sess *ClientSess) createNewChannel(
    inProtocol string,
    onOpenComplete OnOpenComplete,
) {

    info := &pdi.EntryInfo{
        EntryOp: pdi.EntryOp_NEW_CHANNEL_EPOCH,
    }
    info.SetTimeAuthored(0)
    copy(info.AuthorEntryID(), sess.MemberSeed.MemberEpoch.EpochTID)


    chEpoch := pdi.ChannelEpoch{
        ChProtocol: inProtocol,
        ACC: sess.Info.StorageEpoch.RootACC(),
    }

    item := &msgItem{
        msg: sess.newMsg(repo.MsgOp_CH_NEW_ENTRY_READY),
        onCommitComplete: func(
            inEntryInfo *pdi.EntryInfo,
            inEntryState *repo.EntryState,
            inErr error,
        ) {
            if inErr != nil {
                onOpenComplete(nil, inErr)
            } else {
                chSess, err := sess.openChannel(inEntryInfo.FormGenesisChID())
                onOpenComplete(chSess, err)
            }
        },
    }
    item.msg.EntryInfo = info
    item.entryBody, _ = chEpoch.Marshal()

    sess.putResponder(item)

    sess.entriesToCommit <- item

}




func (ws *Workstation) Login(inNum int, inSeedMember bool) error {

    seedPathname := path.Join(ws.UsersPath, ws.Seeds[inNum], seedFilename)

    buf, err := ioutil.ReadFile(seedPathname)
    if err != nil {
        return err
    }

    sess := &ClientSess{
        ws: ws,
        msgOutbox: make(chan *repo.Msg, 4),
        chSessions: make(map[repo.ChSessID]*chSess),
        entriesToCommit: make(chan *msgItem, 1),
    }
    sess.initResponder(sess)

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


    sess.Startup(ws.flow.Ctx)
    if err != nil {
        return err
    }
  
    addr :=  ":50082"
    err = sess.connectToRepo(addr)
    if err != nil {
        return err
    }


    // TODO: move this to ImportSeed()
    if inSeedMember {
        _, err := sess.repoClient.SeedRepo(sess.flow.Ctx, sess.MemberSeed.RepoSeed)
        if err != nil {
            return err

            // TODO close gprc conn
        }    
    }

    err = sess.openMemberSession()
    if err != nil {
        return err
    }

    reader := bufio.NewReader(os.Stdin)
    fmt.Println("ENTER TO START")
    reader.ReadString('\n')

    N := 54

    for i := 0; i < N; i++ {
        idx := i
        go func() {
            sess.Infof(0, "creating new channel %d", idx)
            sess.createNewChannel(repo.ChProtocolTalk, func(
                cs *chSess,
                inErr error,
            ) {
                if err != nil {
                    fmt.Print("createNewChannel error: ", inErr)
                } else {
                    for i := 0; i < 100000; i++ {
                        hello := fmt.Sprintf("hello, world!  This is msg #%d in channel %s", i, cs.ChID.Str())
                        fmt.Println("====> ", hello)

                        cs.PostContent(
                            []byte(hello),
                            func(
                                inEntryInfo *pdi.EntryInfo,
                                inEntryState *repo.EntryState,
                                inErr error,
                            ) {
                                if inErr != nil {
                                    fmt.Println("PostContent() -- got commit hello err:", inErr)
                                } else {
                                    fmt.Println("PostContent() entry ID", inEntryInfo.EntryID().SuffixStr())
                                }
                            },
                        )
                        time.Sleep(10050 * time.Millisecond)
                    }
                }
            })
        }()
    }



    time.Sleep(1000 * time.Second)

    return nil
}



/*

var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."


*/