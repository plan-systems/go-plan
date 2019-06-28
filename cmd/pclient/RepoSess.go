package main

import (
	//"fmt"
	//"context"
	//"io/ioutil"
	//"os"

	//"time"
	//"io"
	//"bufio"
	//"bytes"
	//"fmt"

	//"path"
	//"strconv"
	"sync"
	//"sync/atomic"

	//"github.com/plan-systems/go-plan/ski/Providers/hive"
	//"encoding/hex"
	//crand "crypto/rand"
	//"encoding/json"
	//"strings"

	//"github.com/plan-systems/go-plan/client"
	//"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/repo"
	//"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-ptools"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

/*
type msgItem struct {
	msg              *repo.Msg
	entryBody        []byte
	onCommitComplete OnCommitComplete
}


// MsgSess is characterized by having a list of msg IDs that are currently in progress and have associated responder callbacks.
type MsgSess struct {
	RepoSess        *RepoSess
	chSessID        repo.ChSessID
	responders      map[uint32]*msgItem
	respondersMutex sync.Mutex
	nextMsgID       uint32
}

func (resp *MsgSess) initResponder(rs *RepoSess) {
	resp.responders = map[uint32]*msgItem{}
	resp.RepoSess = rs
	resp.nextMsgID = 1
}

func (resp *MsgSess) newMsg(op repo.MsgOp) *repo.Msg {

	msg := &repo.Msg{
		ID:       atomic.AddUint32(&resp.nextMsgID, 1),
		Op:       op,
		ChSessID: uint32(resp.chSessID),
	}

	return msg
}*/



/*
type chSess struct {
	MsgSess

	ChID     plan.ChID
	msgInbox chan *repo.Msg
	isOpen   bool
}



func (cs *chSess) resetReader() {

	msg := cs.newMsg(repo.MsgOp_RESET_ENTRY_READER)
	msg.FLAGS |= uint32(1) << byte(repo.ChSessionFlags_INCLUDE_BODY)
	msg.FLAGS |= uint32(1) << byte(repo.ChSessionFlags_CONTENT_ENTRIES)


	cs.RepoSess.msgOutbox <- msg
}

func (cs *chSess) CloseSession() {
	if cs.isOpen {
		cs.isOpen = false
		close(cs.msgInbox)
		cs.RepoSess.msgOutbox <- &repo.Msg{
			Op:       repo.MsgOp_CLOSE_CH_SESSION,
			ChSessID: uint32(cs.chSessID),
		}
	}
}*/


// RepoSess is a member using this workstation.
type RepoSess struct {
	ptools.Context

	ws              *WsSession
	repoClient      repo.RepoClient
	msgsToRepo      chan *repo.Msg
	msgInlet        repo.Repo_OpenMemberSessionClient
	msgOutlet       repo.Repo_OpenMsgPipeClient
	sessToken       []byte

	msgJobs			map[uint64]msgJob
    msgJobsMutex    sync.Mutex

}

// NewRepoSess creates a new RepoSess and sets up member crypto.
func NewRepoSess(
	inWsSession *WsSession,
) *RepoSess {

	rs := &RepoSess{
		ws:              inWsSession,
		msgJobs:         make(map[uint64]msgJob),
	}

	return rs
}

// Startup initiates connection to the repo
func (rs *RepoSess) Startup() error {

	rs.SetLogLabel("RepoSess")

	err := rs.CtxStart(
		rs.ctxStartup,
		nil,
		nil,
		rs.ctxStopping,
	)

	return err
}

func (rs *RepoSess) ctxStartup() error {


    err := rs.OpenRepoSession()
    if err != nil {
        rs.Error("OpenRepoSession: ", err)
        return err
    }

	//
	//
	//
    // repo writer/sender
    //
    rs.msgsToRepo = make(chan *repo.Msg, 4)
	rs.CtxGo(func() {

		for msg := range rs.msgsToRepo {
			err := rs.msgOutlet.Send(msg)
			if err != nil {
				if ctxErr := rs.msgOutlet.Context().Err(); ctxErr != nil {
					rs.CtxStop("RepoSess: " + ctxErr.Error(), nil)
				} else { 
					rs.Warn("msgOutlet Send failed: ", err)
				}
			}
		}
	})
	//
	//
	//
    // repo reader/receiver
    //
	rs.CtxGo(func() {
		for rs.CtxRunning() {
			msg, err := rs.msgInlet.Recv()
			if err == nil {
                rs.onMsgFromRepo(msg)
            } else {
				ctxErr := rs.msgInlet.Context().Err()
				if ctxErr != nil {
					rs.CtxStop("RepoSess: " + ctxErr.Error(), nil)
					break
				} else {
					rs.Warnf("RepoSess pipe Recv(): ", err)
				}
            }
        }
    })

	// Send the community keyring!
	{
		msg := &repo.Msg{
			Op: repo.MsgOp_ADD_COMMUNITY_KEYS,
		}

		// TODO: come up w/ better key idea
		msg.BUF0, err = rs.ws.MemberCrypto.ExportCommunityKeyring(rs.sessToken)
		if err != nil {
			return err
		}

		rs.msgsToRepo <- msg
	}

	return nil
}

func (rs *RepoSess) ctxStopping() {
	if rs.msgsToRepo != nil {
		close(rs.msgsToRepo)
	}
}
func (rs *RepoSess) putJob(inJob msgJob) {
    key := uint64(inJob.msg.ID) ^ uint64(inJob.msg.ChSessID)
	rs.msgJobsMutex.Lock()
	rs.msgJobs[key] = inJob
	rs.msgJobsMutex.Unlock()
}

func (rs *RepoSess) reattchBody(ioMsg *repo.Msg) bool {
    key := uint64(ioMsg.ID) ^ uint64(ioMsg.ChSessID)
	rs.msgJobsMutex.Lock()
	job, ok := rs.msgJobs[key]
    if ok {
		delete(rs.msgJobs, key)
	}
	rs.msgJobsMutex.Unlock()

    if ok {
        ioMsg.BUF0 = job.entryBody
    }

	return ok
}

func (rs *RepoSess) onMsgFromRepo(msg *repo.Msg) {

    fwdToClient := true

	switch msg.Op {

        case repo.MsgOp_CH_NEW_ENTRY_READY:
            if len(msg.Error) > 0 {
                // TODO: log err
            } else {           
                if rs.reattchBody(msg) {
                    rs.ws.entriesToCommit <- msg
                    fwdToClient = false
                } else {
                    rs.Warnf("got CH_NEW_ENTRY_READY but msg ID %d not found", msg.ID)
                }
            }
    }

    if fwdToClient {
        rs.ws.msgsToClient <- msg
    }
}




func (rs *RepoSess) connectToRepo() error {

	repoConn, err := grpc.DialContext(rs.Ctx, rs.ws.repoAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	rs.repoClient = repo.NewRepoClient(repoConn)

	return nil
}

func (rs *RepoSess) disconnectFromRepo() {

}

func (rs *RepoSess) OpenRepoSession() error {

	var (
		header metadata.MD
	)

	err := rs.connectToRepo()
	if err != nil {
		return err
	}

	_, err = rs.repoClient.SeedRepo(rs.Ctx, rs.ws.MemberSeat.RepoSeed)
	if err != nil {
		return err
	}

	rs.msgInlet, err = rs.repoClient.OpenMemberSession(rs.Ctx,
		&repo.MemberSessionReq{
			WorkstationID: rs.ws.MemberSeat.SeatID,
			CommunityID:   rs.ws.MemberSeat.GenesisSeed.CommunityEpoch.CommunityID,
			MemberEpoch:   rs.ws.MemberSeat.MemberEpoch,
		},
		grpc.Header(&header),
	)
	if err != nil {
		return err
	}

	msg, err := rs.msgInlet.Recv()
	if err != nil {
		return err
	}
	if msg.Op != repo.MsgOp_MEMBER_SESSION_READY || msg.ChSessID != 0 {
		return plan.Error(nil, plan.AssertFailed, "did not get valid MsgOp_MEMBER_SESSION_READY ")
	}

	rs.sessToken = msg.BUF0

	// Since OpenRepoSession() uses a stream responder, the trailer is never set, so we use this guy as the sesh token.
	rs.Ctx = ptools.ApplyTokenOutgoingContext(rs.Ctx, rs.sessToken)

	rs.msgOutlet, err = rs.repoClient.OpenMsgPipe(rs.Ctx)
	if err != nil {
		return err
	}

		
	return nil
}


/*
// OnCommitComplete is a callback when an entry's txn(s) have been merged or rejected by the repo.
type OnCommitComplete func(
	inEntryInfo *pdi.EntryInfo,
	inEntryState *repo.EntryState,
	inErr error,
)

// OnOpenComplete is callback when a new ch session is now open and available for access.
type OnOpenComplete func(
	chrs *chSess,
	inErr error,
)

func (rs *RepoSess) createNewChannel(
	inProtocol string,
	onOpenComplete OnOpenComplete,
) {

	info := &pdi.EntryInfo{
		EntryOp: pdi.EntryOp_NEW_CHANNEL_EPOCH,
	}
	info.SetTimeAuthored(0)
	copy(info.AuthorEntryID(), rs.MemberSeed.MemberEpoch.EpochTID)

	chEpoch := pdi.ChannelEpoch{
		ChProtocol: inProtocol,
		ACC:        rs.Info.StorageEpoch.RootACC(),
	}

	item := &msgItem{
		msg: rs.newMsg(repo.MsgOp_CH_NEW_ENTRY_READY),
		onCommitComplete: func(
			inEntryInfo *pdi.EntryInfo,
			inEntryState *repo.EntryState,
			inErr error,
		) {
			if inErr != nil {
				onOpenComplete(nil, inErr)
			} else {
				chSess, err := rs.openChannel(inEntryInfo.EntryID().ExtractChID())
				onOpenComplete(chSess, err)
			}
		},
	}
	item.msg.EntryInfo = info
	item.entryBody, _ = chEpoch.Marshal()

	rs.putResponder(item)

	rs.entriesToCommit <- item

}
*/