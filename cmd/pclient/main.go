package main

import (
	//"fmt"
	//"context"
	"flag"
	"io/ioutil"
	"log"
	"os"

	"time"
	//"io"
	"bufio"
	"bytes"
	"fmt"

	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/plan-systems/go-plan/ski/Providers/hive"
	//"encoding/hex"
	crand "crypto/rand"
	"encoding/json"
	"strings"

	"github.com/plan-systems/go-plan/client"
	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/repo"
	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-ptools"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const localPnode = ":" + plan.DefaultRepoServicePort

func main() {

	init 		:= flag.Bool("init", 		false, 				"init creates <datadir> as a fresh/new pclient datastore")
	dataDir 	:= flag.String("datadir", 	"~/_PLAN_pclient", 	"datadir specifies the path for all file access and storage")
	seed 		:= flag.String("seed", 		"", 				"seed reads a PLAN seed file ands seeds a community instance on the host pnode")
	hostAddr 	:= flag.String("host",		localPnode, 		"host specifies the net addr of a host pnode for all connection")
	testRepeat  := flag.String("Trepeat",    "10",				"how often a test msg will be posted to the test channel")
	testCreate  := flag.String("Topen",    	"", 				"opens the given channel (vs creating a new channel)")

	flag.Parse()
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")

	ws, err := NewWorkstation(*dataDir, *init)
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
		err := ws.Startup()
		if err != nil {
			log.Fatalf("failed to startup: %v", err)
		}

		ws.AttachInterruptHandler()

		err = ws.Login(*hostAddr, 0, seedMember)
		if err != nil {
			log.Fatalf("client-sim fatal err: %v", err)
		}
	}

	{
		testDelay := float64(10)
		if testDelay, err = strconv.ParseFloat(*testRepeat, 32); err != nil {
			log.Fatal(err)
		}

		var openChID []byte
		if testCreate != nil && len(*testCreate) > 0 {
			if openChID, err = plan.BinEncoding.DecodeString(*testCreate); err != nil {
				log.Fatal(err)
			}
		}
		if len(openChID) != 0 && len(openChID) != plan.ChIDSz {
			log.Fatal("bad channel ID")
		}

		reader := bufio.NewReader(os.Stdin)
		fmt.Println("ENTER TO START")
		reader.ReadString('\n')

		N := 1
		//waiter := sync.WaitGroup
		//time.Sleep(5 * time.Second)

		for i := 0; i < N; i++ {
			go doTest(ws.ms, openChID, int64(testDelay*1000))
		}

		ws.CtxWait()
	}
}



func doTest(
	ms *MemberSess,
	chID plan.ChID,
	repeatMS int64,
) {

	var (
		//cs *chSess
		//err error
	)
	isRdy := make(chan *chSess, 1)
	if len(chID) == plan.ChIDSz {
		ms.Infof(0, "opening channel %s", chID.Str())
		cs, err := ms.openChannel(chID)
		if err != nil {
			log.Fatal(err)
		}

		isRdy <- cs
	} else {
		ms.createNewChannel(repo.ChProtocolTalk, func(
			inCS *chSess,
			inErr error,
		) {
			if inErr != nil {
				ms.Error("createNewChannel error: ", inErr)
			} else {
				ms.Infof(0, "created new channel %s", inCS.ChID.Str())
			}

			isRdy <- inCS
		})
	}

	cs :=  <- isRdy
	if cs == nil {
		return
	}

	cs.resetReader()

	for i := 1; ms.CtxRunning(); i++ {
		hello := fmt.Sprintf("Hello, Universe #%d, from %s", i, path.Base(ms.ws.BasePath))

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
					//fmt.Println("PostContent() entry ID", inEntryInfo.EntryID().SuffixStr())
				}
			},
		)
		time.Sleep(time.Duration(repeatMS) * time.Millisecond)
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
//	 account on a given pnode for a given community.
type Workstation struct {
	ptools.Context

	BasePath  string
	UsersPath string
	Info      InstallInfo
	Seeds     []string
	ms        *MemberSess
}

// InstallInfo is generated during client installation is considered immutable.
type InstallInfo struct {
	InstallID ptools.Bytes `json:"install_id"`
}

// NewWorkstation creates a new Workstation instance
func NewWorkstation(
	inBasePath string,
	inDoInit bool,
) (*Workstation, error) {

	ws := &Workstation{}
	ws.SetLogLabel("pclient")

	var err error
	if ws.BasePath, err = ptools.SetupBaseDir(inBasePath, inDoInit); err != nil {
		return nil, err
	}

	ws.UsersPath = path.Join(ws.BasePath, "users")
	if err = os.MkdirAll(ws.UsersPath, ptools.DefaultFileMode); err != nil {
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
				err = ioutil.WriteFile(pathname, buf, ptools.DefaultFileMode)
			}
		} else {
			err = plan.Errorf(err, plan.ConfigFailure, "Failed to load client/workstation install info")
		}
	}

	return err
}

// Startup --
func (ws *Workstation) Startup() error {

	err := ws.CtxStart(
		ws.ctxStartup,
		nil,
		nil,
		ws.ctxStopping,
	)

	return err
}

func (ws *Workstation) ctxStartup() error {

	dirs, err := ioutil.ReadDir(ws.UsersPath)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		itemPath := dir.Name()
		if !strings.HasPrefix(itemPath, ".") {
			ws.Seeds = append(ws.Seeds, itemPath)
		}
	}

	return nil
}

func (ws *Workstation) ctxStopping() {

}

type msgCallback struct {
	MsgID uint32
	F     func(inMsg *repo.Msg)
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
	userDir, err = ptools.CreateNewDir(ws.UsersPath, userDir)
	if err != nil {
		return nil, err
	}

	err = ioutil.WriteFile(path.Join(userDir, seedFilename), buf, ptools.DefaultFileMode)
	if err != nil {
		return nil, err
	}

	return seed, nil
}

type msgItem struct {
	msg              *repo.Msg
	entryBody        []byte
	onCommitComplete OnCommitComplete
}

// MsgResponder is characterized by having a list of msg IDs that are currently in progress and have associated responder callbacks.
type MsgResponder struct {
	MemberSess      *MemberSess
	chSessID        repo.ChSessID
	responders      map[uint32]*msgItem
	respondersMutex sync.Mutex
	nextMsgID       uint32
}

func (resp *MsgResponder) initResponder(ms *MemberSess) {
	resp.responders = map[uint32]*msgItem{}
	resp.MemberSess = ms
	resp.nextMsgID = 1
}

func (resp *MsgResponder) newMsg(op repo.MsgOp) *repo.Msg {

	msg := &repo.Msg{
		ID:       atomic.AddUint32(&resp.nextMsgID, 1),
		Op:       op,
		ChSessID: uint32(resp.chSessID),
	}

	return msg
}

func (resp *MsgResponder) putResponder(inItem *msgItem) {
	resp.respondersMutex.Lock()
	resp.responders[inItem.msg.ID] = inItem
	resp.respondersMutex.Unlock()
}

func (resp *MsgResponder) fetchResponder(inMsgID uint32, inPop bool) *msgItem {
	resp.respondersMutex.Lock()
	item := resp.responders[inMsgID]
	if item != nil && inPop {
		delete(resp.responders, inMsgID)
	}
	resp.respondersMutex.Unlock()

	return item
}

func (resp *MsgResponder) handleCommon(msg *repo.Msg) {

	switch msg.Op {

	case repo.MsgOp_CH_NEW_ENTRY_READY:
		if item := resp.fetchResponder(msg.ID, false); item != nil {
			if len(msg.Error) > 0 {
				// TODO: log err
			} else {
				item.msg = msg
				resp.MemberSess.entriesToCommit <- item
			}
		} else {
			resp.MemberSess.Warnf("got CH_NEW_ENTRY_READY but msg ID %d not found", msg.ID)
		}

	case repo.MsgOp_COMMIT_COMPLETE:
		if item := resp.fetchResponder(msg.ID, true); item != nil {
			if item.onCommitComplete != nil {
				var err error
				if len(msg.Error) > 0 {
					err = plan.Error(nil, plan.FailedToCommitTxns, msg.Error)
				}
				go item.onCommitComplete(msg.EntryInfo, msg.EntryState, err)
			}
		} else {
			resp.MemberSess.Warnf("got MsgOp_COMMIT_COMPLETE but msg ID %d not found", msg.ID)
		}
	}
}

type chSess struct {
	MsgResponder

	ChID     plan.ChID
	msgInbox chan *repo.Msg
	isOpen   bool
}


func (cs *chSess) PostContent(
	inBody []byte,
	onCommitComplete OnCommitComplete,
) {

	msg := cs.newMsg(repo.MsgOp_CH_NEW_ENTRY_REQ)
	msg.EntryInfo = &pdi.EntryInfo{
		EntryOp: pdi.EntryOp_POST_CONTENT,
	}

	cs.putResponder(&msgItem{
		msg:              msg,
		entryBody:        inBody,
		onCommitComplete: onCommitComplete,
	})

	cs.MemberSess.msgOutbox <- msg
}

func (cs *chSess) resetReader() {

	msg := cs.newMsg(repo.MsgOp_RESET_ENTRY_READER)
	msg.FLAGS |= uint32(1) << byte(repo.ChSessionFlags_INCLUDE_BODY)
	msg.FLAGS |= uint32(1) << byte(repo.ChSessionFlags_CONTENT_ENTRIES)
	/*cs.putResponder(&msgItem{
		msg: msg,
	})*/

	cs.MemberSess.msgOutbox <- msg
}

func (cs *chSess) CloseSession() {
	if cs.isOpen {
		cs.isOpen = false
		close(cs.msgInbox)
		cs.MemberSess.msgOutbox <- &repo.Msg{
			Op:       repo.MsgOp_CLOSE_CH_SESSION,
			ChSessID: uint32(cs.chSessID),
		}
	}
}

// MemberSess is a member using this workstation.
type MemberSess struct {
	ptools.Context
	MsgResponder

	MemberCrypto    client.MemberCrypto
	MemberSeed      repo.MemberSeed
	Info            repo.GenesisSeed
	ws              *Workstation
	repoClient      repo.RepoClient
	entriesToCommit chan *msgItem
	msgOutbox       chan *repo.Msg
	msgInlet        repo.Repo_OpenMemberSessionClient
	msgOutlet       repo.Repo_OpenMsgPipeClient
	sessToken       []byte
	chSessions      map[repo.ChSessID]*chSess
	chSessionsMutex sync.RWMutex
}

// NewMemberSess creates a new MemberSess and sets up member crypto.
func NewMemberSess(
	ws *Workstation,
	seedPathname string,
) (*MemberSess, error) {

	buf, err := ioutil.ReadFile(seedPathname)
	if err != nil {
		return nil, err
	}

	ms := &MemberSess{
		ws:              ws,
		msgOutbox:       make(chan *repo.Msg, 4),
		chSessions:      make(map[repo.ChSessID]*chSess),
		entriesToCommit: make(chan *msgItem, 1),
	}
	ms.initResponder(ms)

	if err = ms.MemberSeed.Unmarshal(buf); err != nil {
		return nil, err
	}

	// TOD: This could be send over the wire as a Msg from the WsClient.
	{
		unpacker := ski.NewUnpacker(false)

		var packingInfo ski.SignedPayload
		err = unpacker.UnpackAndVerify(ms.MemberSeed.RepoSeed.SignedGenesisSeed, &packingInfo)
		if err == nil {
			err = ms.Info.Unmarshal(packingInfo.Header)
			if err == nil {
				if ! bytes.Equal(packingInfo.Signer.PubKey, ms.Info.StorageEpoch.OriginKey.PubKey) {
					err = plan.Errorf(nil, plan.VerifySignatureFailed, "failed to verify %v", seedPathname)
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return ms, nil
}

// Startup initiates connection to the repo
func (ms *MemberSess) Startup() error {

	ms.SetLogLabel("MemberSession")

	err := ms.CtxStart(
		ms.ctxStartup,
		nil,
		nil,
		ms.ctxStopping,
	)

	return err
}

func (ms *MemberSess) ctxStartup() error {

	ms.MemberCrypto.CommunityEpoch = *ms.Info.CommunityEpoch
	ms.MemberCrypto.StorageEpoch = *ms.Info.StorageEpoch

	keyDir, err := hive.GetSharedKeyDir()
	if err != nil {
		return err
	}

	SKI, err := hive.StartSession(
		keyDir,
		ms.MemberSeed.MemberEpoch.FormMemberStrID(),
		[]byte("password"),
	)
	if err != nil {
		return err
	}

	err = ms.MemberCrypto.StartSession(SKI, *ms.MemberSeed.MemberEpoch)
	if err != nil {
		return err
	}

	//
	//
	//
	ms.CtxGo(func() {

		for item := range ms.entriesToCommit {
			txnSet, err := ms.MemberCrypto.EncryptAndEncodeEntry(item.msg.EntryInfo, item.entryBody)
			if err != nil {
				ms.Warn("failed to encypt/encode entry: ", err)

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
				ms.msgOutbox <- msg
			}
		}

		ms.MemberCrypto.EndSession(ms.CtxStopReason())
	})

	//
	//
	//
	ms.CtxGo(func() {

		for msg := range ms.msgOutbox {

			//ms.Infof(1, "ms.msgOutlet -> (sess %d, ID %d, %s)", msg.ChSessID, msg.ID, repo.MsgOp_name[int32(msg.Op)])

			err := ms.msgOutlet.Send(msg)
			if err != nil {
				ms.Warn("error sending msg: ", err)

				// TODO: save msg to failed to send log/db?
			}
		}

		ms.MemberCrypto.EndSession(ms.CtxStopReason())

	})

	return nil
}

func (ms *MemberSess) ctxStopping() {

	// First end all chSessions
	ms.chSessionsMutex.RLock()
	for _, cs := range ms.chSessions {
		cs.CloseSession()
	}
	ms.chSessionsMutex.RUnlock()

	if ms.msgOutbox != nil {
		close(ms.msgOutbox)
	}
}

func (ms *MemberSess) detachChSess(cs *chSess) {

	ms.chSessionsMutex.Lock()
	delete(ms.chSessions, cs.chSessID)
	ms.chSessionsMutex.Unlock()

}

func (ms *MemberSess) connectToRepo(inAddr string) error {

	repoConn, err := grpc.DialContext(ms.Ctx, inAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	ms.repoClient = repo.NewRepoClient(repoConn)

	return nil
}

func (ms *MemberSess) disconnectFromRepo() {

}

func (ms *MemberSess) openMemberSession() error {

	var (
		header metadata.MD
		err    error
	)

	ms.msgInlet, err = ms.repoClient.OpenMemberSession(ms.Ctx,
		&repo.MemberSessionReq{
			WorkstationID: ms.ws.Info.InstallID,
			CommunityID:   ms.Info.CommunityEpoch.CommunityID,
			MemberEpoch:   ms.MemberSeed.MemberEpoch,
		},
		grpc.Header(&header),
	)
	if err != nil {
		return err
	}

	msg, err := ms.msgInlet.Recv()
	if err != nil {
		return err
	}
	if msg.Op != repo.MsgOp_MEMBER_SESSION_READY || msg.ChSessID != 0 {
		return plan.Error(nil, plan.AssertFailed, "did not get valid MsgOp_MEMBER_SESSION_READY ")
	}

	ms.sessToken = msg.BUF0

	// Since OpenMemberSession() uses a stream responder, the trailer is never set, so we use this guy as the sesh token.
	ms.Ctx = ptools.ApplyTokenOutgoingContext(ms.Ctx, ms.sessToken)

	ms.msgOutlet, err = ms.repoClient.OpenMsgPipe(ms.Ctx)
	if err != nil {
		return err
	}

	// Send the community keyring!
	{
		msg := &repo.Msg{
			Op: repo.MsgOp_ADD_COMMUNITY_KEYS,
		}

		// TODO: come up w/ better key idea
		msg.BUF0, err = ms.MemberCrypto.ExportCommunityKeyring(ms.sessToken)
		if err != nil {
			return err
		}

		ms.msgOutbox <- msg
	}

	go func() {
		for ms.CtxRunning() {
			msg, err := ms.msgInlet.Recv()
			if err == nil {

				//ms.Infof(1, "(sess %d, ID %d, %s) <- ms.msgInlet", msg.ChSessID, msg.ID, repo.MsgOp_name[int32(msg.Op)])

				if msg.ChSessID == 0 {
					ms.handleCommon(msg)
				} else {
					ms.chSessionsMutex.RLock()
					cs := ms.chSessions[repo.ChSessID(msg.ChSessID)]
					ms.chSessionsMutex.RUnlock()
					if cs == nil {
						ms.Warnf("received unrecognized ch session ID: %d", msg.ChSessID)
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

func (ms *MemberSess) openChannel(
	inChID plan.ChID,
) (*chSess, error) {

	sessInfo, err := ms.repoClient.StartChannelSession(ms.Ctx,
		&repo.ChInvocation{
			ChID: inChID,
		})

	if err != nil {
		return nil, err
	}

	cs := &chSess{
		ChID:     inChID,
		msgInbox: make(chan *repo.Msg, 1),
		isOpen:   true,
	}
	cs.initResponder(ms)
	cs.chSessID = repo.ChSessID(sessInfo.SessID)

	ms.chSessionsMutex.Lock()
	if ms.chSessions[cs.chSessID] != nil {
		err = plan.Error(nil, plan.AssertFailed, "ch session ID already in use")
	}
	ms.chSessions[cs.chSessID] = cs
	ms.chSessionsMutex.Unlock()

	if err != nil {
		return nil, err
	}

	go func() {

		for {
			msg := <-cs.msgInbox
			if msg == nil {
				cs.MemberSess.detachChSess(cs) // If we're here, it's bc the channel is closing
				break
			}

			switch msg.Op {
			case repo.MsgOp_CH_ENTRY:
				t := time.Unix(msg.EntryInfo.EntryID().ExtractTime(), 0)
				fmt.Printf("ChSess#%d %s %s: %s\n", msg.ChSessID, msg.EntryInfo.EntryID().SuffixStr(), t.Format("Jan 02 3:04:05"), string(msg.BUF0))

			case repo.MsgOp_CH_SESSION_CLOSED:
				cs.CloseSession()
			default:
				cs.handleCommon(msg)
			}
		}
	}()

	return cs, nil
}


// OnCommitComplete is a callback when an entry's txn(s) have been merged or rejected by the repo.
type OnCommitComplete func(
	inEntryInfo *pdi.EntryInfo,
	inEntryState *repo.EntryState,
	inErr error,
)

// OnOpenComplete is callback when a new ch session is now open and available for access.
type OnOpenComplete func(
	chms *chSess,
	inErr error,
)

func (ms *MemberSess) createNewChannel(
	inProtocol string,
	onOpenComplete OnOpenComplete,
) {

	info := &pdi.EntryInfo{
		EntryOp: pdi.EntryOp_NEW_CHANNEL_EPOCH,
	}
	info.SetTimeAuthored(0)
	copy(info.AuthorEntryID(), ms.MemberSeed.MemberEpoch.EpochTID)

	chEpoch := pdi.ChannelEpoch{
		ChProtocol: inProtocol,
		ACC:        ms.Info.StorageEpoch.RootACC(),
	}

	item := &msgItem{
		msg: ms.newMsg(repo.MsgOp_CH_NEW_ENTRY_READY),
		onCommitComplete: func(
			inEntryInfo *pdi.EntryInfo,
			inEntryState *repo.EntryState,
			inErr error,
		) {
			if inErr != nil {
				onOpenComplete(nil, inErr)
			} else {
				chSess, err := ms.openChannel(inEntryInfo.EntryID().ExtractChID())
				onOpenComplete(chSess, err)
			}
		},
	}
	item.msg.EntryInfo = info
	item.entryBody, _ = chEpoch.Marshal()

	ms.putResponder(item)

	ms.entriesToCommit <- item

}

// Login is a test login
func (ws *Workstation) Login(inHostAddr string, inNum int, inSeedMember bool) error {

	seedPathname := path.Join(ws.UsersPath, ws.Seeds[inNum], seedFilename)

	var err error
	ws.ms, err = NewMemberSess(ws, seedPathname)

	ws.ms.Startup()
	if err != nil {
		return err
	}

	err = ws.ms.connectToRepo(inHostAddr)
	if err != nil {
		return err
	}

	// TODO: move this to ImportSeed()
	if inSeedMember {
		_, err := ws.ms.repoClient.SeedRepo(ws.ms.Ctx, ws.ms.MemberSeed.RepoSeed)
		if err != nil {
			return err

			// TODO close gprc conn
		}
	}

	err = ws.ms.openMemberSession()
	if err != nil {
		return err
	}

	return nil
}


/*
// REMOVE ME

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("ENTER TO START")
	reader.ReadString('\n')

	N := 1
	//waiter := sync.WaitGroup
	//time.Sleep(5 * time.Second)

	for i := 0; i < N; i++ {
		idx := i
		go func() {
			ms.Infof(0, "creating new channel %d", idx)
			ms.createNewChannel(repo.ChProtocolTalk, func(
				cs *chSess,
				inErr error,
			) {
				if err != nil {
					fmt.Print("createNewChannel error: ", inErr)
				} else {
					for i := 1; i < 100000; i++ {
						hello := fmt.Sprintf("Hello, Universe!  This is msg #%d in channel %s", i, cs.ChID.SuffixStr())
						//fmt.Println("=======> ", hello)

						if i == 6 {
							cs.readerTest()
						}

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
						time.Sleep(5050 * time.Millisecond)

					}
				}
			})
		}()
	}

	reader.ReadString('\n')

	time.Sleep(1000 * time.Second)

	return nil
}
*/