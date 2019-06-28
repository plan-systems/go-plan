package main

import (
	"fmt"
	"bytes"
	"context"
	//"flag"
	"io/ioutil"
	//"log"
	"os"

	//"time"
	//"io"
	//"bufio"
	//"bytes"
	//"fmt"

	"path"
	//"strconv"
	//"sync"
	//"sync/atomic"

    ds "github.com/plan-systems/go-plan/cmd/pdi-local/datastore"

	"github.com/plan-systems/go-plan/ski/Providers/hive"
	//"encoding/hex"
	crand "crypto/rand"
	"encoding/json"
	//"strings"

	"github.com/plan-systems/go-plan/client"
	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/repo"
	//"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-ptools"

	"google.golang.org/grpc"
	//"google.golang.org/grpc/metadata"
)


const (
	seedFilename = "seat.MemberSeed"
	seatFilename = "seat.MemberSeat"
	hiveFilename = "member.hive"
)


// WsHost represents a client "install" on workstation or mobile device.
// It's distinguished by its ability to connect to a pnode and become "a dumb terminal"
// for the given pnode.  Typically, the PLAN is on the same physical device as a pnode
// so that the file system is shared, but this is not required.
//
// For a given PLAN install on a given workstation/device:
// (a) Only one person ("user") is permitted to use it at a time.
// (b) Any number of persons/agents can use it (like user accounts on a traditional OS)
// (c) A User has any number of communities (repos) seeded, meaning it has established an
//	 account on a given pnode for a given community.
type WsHost struct {
	ptools.Context

	BasePath   string
	SeatsPath  string
	Info	   InstallInfo
	grpcServer *grpc.Server
	grpcPort   string
	repoAddr   string
}

// InstallInfo is generated during client installation is considered immutable.
type InstallInfo struct {
	InstallID ptools.Bytes `json:"install_id"`
}

// NewWsHost creates a new WsHost instance
func NewWsHost(
	inBasePath string,
	inDoInit bool,
	inPort string,
	inRepoAddr string,
) (*WsHost, error) {

	ws := &WsHost{
		grpcPort: inPort,
		repoAddr: inRepoAddr,
	}
	ws.SetLogLabel("pclient")

	var err error
	if ws.BasePath, err = plan.SetupBaseDir(inBasePath, inDoInit); err != nil {
		return nil, err
	}

	ws.SeatsPath = path.Join(ws.BasePath, "seats")
	if err = os.MkdirAll(ws.SeatsPath, plan.DefaultFileMode); err != nil {
		return nil, err
	}

	if err = ws.readConfig(inDoInit); err != nil {
		return nil, err
	}

	return ws, nil
}

func (ws *WsHost) readConfig(inFirstTime bool) error {

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
func (ws *WsHost) Startup() error {

	err := ws.CtxStart(
		ws.ctxStartup,
		nil,
		nil,
		ws.ctxStopping,
	)

	return err
}

func (ws *WsHost) ctxStartup() error {

	var err error
/*
	dirs, err := ioutil.ReadDir(ws.SeatsPath)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		itemPath := dir.Name()
		if !strings.HasPrefix(itemPath, ".") {
			ws.Seeds = append(ws.Seeds, itemPath)
		}
	}
*/
	//
	//
	//
	// grpc service
	//
	if err == nil {
		ws.grpcServer = grpc.NewServer()
		client.RegisterWsServiceServer(ws.grpcServer, ws)		// TODO: secure only!
		
		err = ws.AttachGrpcServer(
			"tcp",
			":" + ws.grpcPort,
			ws.grpcServer,
		)
	}

	return nil
}



func (ws *WsHost) ctxStopping() {

}



func (ws *WsHost) CreateNewSeatFromSeed(
	inSeed *client.MemberSeed, 
) (*client.MemberSeat, error) {

	seat := &client.MemberSeat{
		SeatID: make([]byte, plan.WorkstationIDSz),
		MemberEpoch: inSeed.MemberEpoch,
		RepoSeed: inSeed.RepoSeed,
	}

	crand.Read(seat.SeatID)

	var err error
	seat.GenesisSeed, err = repo.ExtractAndVerifyGenesisSeed(inSeed.RepoSeed.SignedGenesisSeed)
	if err != nil {
		return nil, err
	}

	seatDir := plan.BinEncode(seat.SeatID)
	seatPath, err := plan.CreateNewDir(ws.SeatsPath, seatDir)
	if err != nil {
		return nil, err
	}

	seedBuf, err := inSeed.Marshal()
	if err == nil {
		err = ioutil.WriteFile(path.Join(seatPath, seedFilename), seedBuf, plan.DefaultFileMode)
	}
	if err != nil {
		return nil, err
	}

	seatBuf, err := seat.Marshal()
	if err == nil {
		err = ioutil.WriteFile(path.Join(seatPath, seatFilename), seatBuf, plan.DefaultFileMode)
	}
	if err != nil {
		return nil, err
	}

	err = ioutil.WriteFile(path.Join(seatPath, hiveFilename), inSeed.KeyTome, plan.DefaultFileMode)
	if err != nil {
		return nil, err
	}

	ws.Infof(0, "seated member ID=%d from community %v (%s) at %s", 
		inSeed.MemberEpoch.MemberID, 
		plan.BinEncode(seat.GenesisSeed.CommunityEpoch.CommunityID),
		seat.GenesisSeed.StorageEpoch.Name,
		seatPath,
	)

	return seat, nil
}



func (ws *WsHost) CreateNewSeatFromSeedFile(
	inPathname string,
) (*client.MemberSeat, error) {

	seedBuf, err := ioutil.ReadFile(inPathname)
	if err != nil {
		return nil, err
	}

	seed := &client.MemberSeed{}
	err = seed.Unmarshal(seedBuf)
	if err != nil {
		return nil, err
	}

	return ws.CreateNewSeatFromSeed(seed)
}


// CreateNewSeat -- see service WsService
func (ws *WsHost) CreateNewSeat(
	inCtx context.Context,
	inSeatReq *client.MemberSeatReq, 
) (*client.MemberSeat, error) {

	return ws.CreateNewSeatFromSeed(inSeatReq.MemberSeed)
}



// OpenWsSession -- see service WsService
func (ws *WsHost) OpenWsSession(
	ioPipe client.WsService_OpenWsSessionServer, 
) error {

	msg, err := ioPipe.Recv()
	if err != nil {
		return err
	}

	if msg.Op != repo.MsgOp_LOGIN_TO_WS_SEAT {
		return plan.Error(nil, plan.ParamErr, "expected LOGIN_TO_WS_SEAT")
	}

	login := &client.WsLogin{}
	err = login.Unmarshal(msg.BUF0)
	if err != nil {
		return err
	}

	sess, err := ws.StartNewWsSession(ws.SeatsPath, login, ioPipe, ws.repoAddr)
	if err != nil {
		return err
	}

	<-sess.CtxStopping()

	return nil
}

// StartChannelSession -- see service WsService
func (ws *WsHost) StartChannelSession(
	ctx context.Context, 
	inInvocation *repo.ChInvocation, 
) (*repo.ChSessionInfo, error) {
	/*
	ms, err := pn.fetchRepoSession(ctx)
	if err != nil {
		return nil, err
	}*?

	chSession, err := ms.StartChannelSession(inInvocation)
	if err != nil {
		return nil, err
	}

	info := &repo.ChSessionInfo{
		SessID: 0, //uint32(chSession.ChSessID),
	}

	return info, nil
}



type msgJobID struct {
	MsgID			uint32
	ChSessID 		uint32
}

type msgJob struct {
	msg              *repo.Msg
	entryBody        []byte
}


// WsSession represents a user/member "logged in", meaning a SKI session is active.
type WsSession struct {
	ptools.Context

	SessionToken	[]byte
	SeatID		  []byte
	SeatPath		string
	MemberSeat	  client.MemberSeat

	msgsToClient	chan *repo.Msg

	entriesToCommit chan *repo.Msg
	MemberCrypto    pdi.MemberCrypto
	msgOutbox	   	chan *repo.Msg
	wsPipe		  	client.WsService_OpenWsSessionServer
	login			*client.WsLogin
	repoAddr		string
	repoSess		*RepoSess

}


func (ws *WsHost) StartNewWsSession(
	inSeatsPath string,
	inLogin *client.WsLogin,
	ioPipe client.WsService_OpenWsSessionServer, 
	inRepoAddr string,
) (*WsSession, error) {

	if len(inLogin.SeatID) != plan.WorkstationIDSz {
		return nil, plan.Error(nil, plan.ParamErr, "bad workstation ID")
	}

	seatIDStr := plan.BinEncode(inLogin.SeatID)

	sess := &WsSession{
		//WsHostID: inSessReq.WsHostID,
		SessionToken: make([]byte, 18),
		SeatPath: path.Join(inSeatsPath, seatIDStr),
		wsPipe: ioPipe,
		login: inLogin,
		repoAddr: inRepoAddr,
	}

	sess.SetLogLabel(fmt.Sprint("WsSess ", seatIDStr[:4]))

	seatBuf, err := ioutil.ReadFile(path.Join(sess.SeatPath, seatFilename))
	if err == nil {
		err = sess.MemberSeat.Unmarshal(seatBuf)
	}
	if err != nil {
		return nil, err
	}

	if ! bytes.Equal(inLogin.SeatID, sess.MemberSeat.SeatID) {
		return nil, plan.Error(nil, plan.ParamErr, "intneral seat ID mismatch")
	}

	err = sess.CtxStart(
		sess.ctxStartup,
		nil,
		nil,
		sess.ctxStopping,
	)
	if err != nil {
		return nil, err
	}

	return sess, nil
}



func (sess *WsSession) ctxStartup() error {

	seed := sess.MemberSeat.GenesisSeed

	sess.MemberCrypto = pdi.MemberCrypto{
		CommunityEpoch: *seed.CommunityEpoch,
		StorageEpoch:   *seed.StorageEpoch,
		TxnEncoder: ds.NewTxnEncoder(false, *seed.StorageEpoch),
		MemberEpoch: *sess.MemberSeat.MemberEpoch,
	}

	SKI, err := hive.StartSession(
		sess.SeatPath,
		hiveFilename,
		sess.login.Pass,
	)
	ptools.Zero(sess.login.Pass)
	if err != nil {
		return err
	}

	err = sess.MemberCrypto.StartSession(SKI)
	if err != nil {
		return err
	}

	sess.repoSess = NewRepoSess(sess)
	err = sess.repoSess.Startup()
	if err != nil {
		return err
	}

	//
	//
	// entry encryption
	//
	sess.entriesToCommit = make(chan *repo.Msg, 1)
	sess.CtxGo(func() {

		for msg := range sess.entriesToCommit {
			txnSet, err := sess.MemberCrypto.EncryptAndEncodeEntry(msg.EntryInfo, msg.BUF0)
			if err != nil {
				sess.Warn("failed to encrypt/encode entry: ", err)    // TODO: save txns? to failed to send log/db?
			} else {
				msg.Op = repo.MsgOp_COMMIT_TXNS
				msg.EntryInfo = nil
				msg.EntryState = nil
				msg.BUF0 = nil
				N := len(txnSet.Segs)
				msg.ITEMS = make([][]byte, N)
				for i := 0; i < N; i++ {
					msg.ITEMS[i] = txnSet.Segs[i].RawTxn
				}
				sess.repoSess.msgsToRepo <- msg
			}
		}

		sess.MemberCrypto.EndSession(sess.CtxStopReason())
	})
	//
	//
	// 
	// Client msg reader/receiver
	//
	sess.CtxGo(func() {

		for sess.CtxRunning() {
			msg, err := sess.wsPipe.Recv()
			if err == nil {
				sess.onMsgFromClient(msg)
			} else {
				ctxErr := sess.wsPipe.Context().Err()
				if ctxErr != nil {
					sess.CtxStop("WsSession: " + ctxErr.Error(), nil)
					break
				} else {
					sess.Warnf("WsSession pipe Recv(): ", err)
				}
			}
		}

		sess.Info(2, "Client msg reader/receiver EXITING")
	})
	//
	//
	// 
	// Client msg writer/sender
	//
	sess.msgsToClient = make(chan *repo.Msg, 4)
	sess.CtxGo(func() {

		for msg := range sess.msgsToClient {
			err := sess.wsPipe.Send(msg)
			if err != nil {
				if ctxErr := sess.wsPipe.Context().Err(); ctxErr != nil {
					sess.CtxStop("WsSession: " + ctxErr.Error(), nil)
				} else { 
					sess.Warn("wsPipe.Send failed: ", err)
				}
			}
		}

		sess.Info(2, "Client msg writer/sender EXITING")
	})

	return nil
}



func (sess *WsSession) ctxStopping() {

	// With all the channel sessions stopped, we can safely close their outlet, causing a close-cascade.
	if sess.msgsToClient != nil {
		close(sess.msgsToClient)
	}
}

func (sess *WsSession) onMsgFromClient(msg *repo.Msg) { 

	fwdToRepo := true

    switch msg.Op {

		case repo.MsgOp_POST_CH_ENTRY:
            body := msg.BUF0
            msg.BUF0 = nil
            sess.repoSess.putJob(msgJob{
                msg: msg,
                entryBody: body,
            })

		case repo.MsgOp_CH_GENESIS_ENTRY:
		    msg.EntryInfo.SetTimeAuthored(0)
    		msg.EntryInfo.AuthorSig = nil
    		copy(msg.EntryInfo.AuthorEntryID(), sess.MemberCrypto.MemberEpoch.EpochTID)

			// No need to send entries to the repo for channel genesis
			sess.entriesToCommit <- msg
			fwdToRepo = false

    }

    if fwdToRepo {
		sess.repoSess.msgsToRepo <- msg
	}
}



/*

// Login is a test login
func (ws *WsHost) Login(inHostAddr string, inNum int, inSeedMember bool) error {

	seedPathname := path.Join(ws.SeatsPath, ws.Seeds[inNum], seedFilename)

	var err error
	ws.ms, err = NewRepoSess(ws, seedPathname)

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

	err = ws.ms.openRepoSession()
	if err != nil {
		return err
	}

	return nil
}
*/