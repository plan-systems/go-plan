package repo

import (
	crypto_rand "crypto/rand"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    //"time"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"context"
    "fmt"
    
    "github.com/plan-systems/go-plan/ski/Providers/hive"

    ds "github.com/plan-systems/go-plan/cmd/pdi-local/datastore"

    "github.com/plan-systems/go-ptools"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"

    //"github.com/dgraph-io/badger"

)

/*

Huge!

Every Repo maintains a list of when the latest entry from a given member has been witnessed.  

this prevents any member from holding onto a "submarine" entry intended to disrupt the community etc.

entries that are older than a set const delta from the latest post are auto-rejected since there's no way
one entry would be offline for weeks/months while *behind* another that is live.
*************

Newly authoered entries sit around until there's a storage provider 

*/

/*


// Shutdown calls Shutdown() on each MemberSession and blocks until all sessions have completed shutting down.
func (mgr *MemberSessMgr) Shutdown(
    inReason string,
    inBlocker *sync.WaitGroup,
) {

    activeSessions := sync.WaitGroup{}

    for {
        mgr.Lock()
        N := len(mgr.List)
        mgr.CR.Infof(1, "ending %d member sessions", N)
        activeSessions.Add(N)
        for i := 0; i < N; i++ {
            ms := mgr.List[i]
            go func() {
                ms.EndSession(inReason)
                activeSessions.Done()
            }()
        }
        mgr.Unlock()

        if N == 0 {
            break
        }

        activeSessions.Wait()
    }

    if inBlocker != nil {
       inBlocker.Done()
    }
}


// detachSession is called after a MemberSession has completed shutting down
//
// THREADSAFE
func (mgr *MemberSessMgr) detachSession(inSess *MemberSession) {

    mgr.Lock()
    N := len(mgr.List)
    for i := 0; i < N; i++ {
        if mgr.List[i] == inSess {
            N--
            mgr.List[i] = mgr.List[N]
            mgr.List[N] = nil
            mgr.List = mgr.List[:N]
            break
        }
    }
    mgr.Unlock()

}
*/


// NewMemberSession sets up a MemberSession for use.
//
// TODO: close inSKI if an error is returned
func NewMemberSession(
    CR *CommunityRepo,
    inSessReq *MemberSessionReq,
    inMsgOutlet Repo_OpenMemberSessionServer,
) (*MemberSession, error) {

    // TODO: close inSKI if an error is returned
    var err error

    if len(inSessReq.WorkstationID) > 0 && len(inSessReq.WorkstationID) != plan.WorkstationIDSz {
        return nil, plan.Error(nil, plan.AssertFailed, "invalid workstation ID")
    }

/*
TODO: check than a session w/ the same member ID + warksation isn't already open?
    // Is the session already open?
    {
        var match *MemberSession

        mgr.RLock()
        for _, ms := range mgr.List {
            if ms.MemberEpoch.MemberID == inSessReq.MemberEpoch.MemberID {
                match = ms
                break 
            }
        }
        mgr.RUnlock()

        if match != nil {
            // TODO end/close existing session
            return nil, plan.Error(nil, plan.AssertFailed, "session already open")
        }
    }
*/

    ms := &MemberSession{
        CR: CR,
        WorkstationID: inSessReq.WorkstationID,
        MemberEpoch: *inSessReq.MemberEpoch,
        SessionToken: make([]byte, 18),
        msgOutlet: inMsgOutlet,
        txnDecoder: ds.NewTxnDecoder(true),
        commCrypto: &CommunityCrypto{

        },
    }

    crypto_rand.Read(ms.SessionToken)

    ms.MemberIDStr = ms.MemberEpoch.FormMemberStrID()
    ms.SetLogLabel(fmt.Sprintf("%s/member %d", CR.GetLogLabel(), inSessReq.MemberEpoch.MemberID))
    ms.SharedPath = path.Join(CR.HomePath, ms.MemberIDStr)

    if len(inSessReq.WorkstationID) == 0 {
        ms.WorkstationPath = ms.SharedPath
    } else {
        ms.WorkstationPath = path.Join(ms.SharedPath, plan.BinEncode(ms.WorkstationID[:15]))
    }
    
    err = ms.CtxStart(
        ms.ctxStartup,
        nil,
        ms.ctxChildAboutToStop,
        ms.ctxStopping,
    )

    if err != nil {
        return nil, err
    }

    ms.msgOutbox <- &Msg{
        Op: MsgOp_MEMBER_SESSION_READY,
        BUF0: ms.SessionToken,
    }

    return ms, nil
}



// The Unity client and repo process are the "same" machine in that they are bound at the hip like simese twins
// for their existence, swapping session tokens from StartSession() to StartSession(). 
// The MemberClient and Unint Client may not be on the same machine, but the sessions are lockstep.
// The other main reason the the are on the same machine is b/c of the LOCAL FILE SYSTEM.  Otherwise, heavy
//     file object and graphics assets can't be transferred.  The CFI 

/*
type MemberClientLifetimeSession struct {
type MemberClient struct {
type MemberTerminal struct {

    //repoPath/memberID/unityID/sessionSig/  => 

*/


// CommunityCrypto wraps a community keyring session.
type CommunityCrypto struct {

    // The current community epoch
    //communityEpoch  pdi.CommunityEpoch

    RetainUpto      int64
    Keys            ski.Session

}



func (cc *CommunityCrypto) CommunityEncrypt(
    inBuf       []byte,
    inKeyRef    *ski.KeyRef,
) ([]byte, error) {

    out, err := cc.Keys.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        OpKey: inKeyRef,
        BufIn: inBuf,
    })
    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}


func (cc *CommunityCrypto) EndSession(
    inReason string,
) {

    if cc.Keys != nil {
        cc.Keys.EndSession(inReason)
        cc.Keys = nil
    }
}






// MemberSession represents a user/member "logged in", meaning a SKI session is active.
type MemberSession struct {
    ptools.Context

    CR              *CommunityRepo

    // Pathname to member repo files (shared by all client instances)
    SharedPath      string

    // Pathname to member repo files specific to a member client/workstation ID.
    WorkstationPath string

    // Uniquely identifies the client install instance
    WorkstationID   []byte

    SessionToken    []byte

    // Ah, a contraction?
    // Nay, the MemberClient more long-winded name is the *MemberClientLifetimeSession*
    // Sequence of sigs/token exchanged by Unity client and here, ensuring that there can't be an imposter session w/o
    //    the member from knowing it next login.   
    // Note that both non-time-dependent (TLS cert exchange) and this approach feature a no-password UX (though a device
    //    may have a device pin to nerf anonymous easy physical theft, etc).
    //sessionDB      *badger.DB
    // Host 


    MemberEpoch     pdi.MemberEpoch
    MemberIDStr     string


    txnDecoder      pdi.TxnDecoder
    //PersonalSKI     ski.Session

    commCrypto      *CommunityCrypto

    ChSessionsCount sync.WaitGroup

    ChSessionsMutex sync.RWMutex
    ChSessions      map[ChSessID]*ChSession

    // Outbound entries from channel adapters to be committed to the community's storage provider(s)
    msgOutbox       chan *Msg
    msgOutlet       Repo_OpenMemberSessionServer

}



func (ms *MemberSession) ctxStartup() error {

    ms.ChSessions = make(map[ChSessID]*ChSession)

    var err error
    if err = os.MkdirAll(ms.WorkstationPath, ms.CR.DefaultFileMode); err != nil {
        return err
    }

    // Create a heap-only key hive used for the community keyring
    if ms.commCrypto.Keys, err = hive.StartSession("", "", nil); err != nil {
        return err
    }

    ms.CR.registerCommCrypto(ms.commCrypto)

    //
    // 
    // outbound client msg sender
    //
    ms.msgOutbox = make(chan *Msg, 8)
    ms.CtxGo(func() {

        isRunning := true

        outletDone := ms.msgOutlet.Context().Done()

        for isRunning {
            var (
                msg *Msg
                err error
            )

            select {
                case msg = <-ms.msgOutbox:
                    if msg != nil {
                        err = ms.msgOutlet.Send(msg)
                        if err != nil {
                            ms.Warn("ms.msgOutlet.Send: ", err)
                        }
                    } else {
                        isRunning = false
                    }
                case <-outletDone:
                    err = ms.msgOutlet.Context().Err()
                    ms.CtxStop("MemberSession: " + err.Error(), nil)
                    outletDone = nil
            }
        }

        ms.CR.unregisterCommCrypto(ms.commCrypto)
    })

    return nil
}


func (ms *MemberSession) ctxChildAboutToStop(inChild ptools.Ctx) {
    cs := inChild.(*ChSession)
    ms.ChSessionsMutex.Lock()
    delete(ms.ChSessions, cs.ChSessID)
    ms.ChSessionsMutex.Unlock()
}

func (ms *MemberSession) ctxStopping() {
    ms.Info(2, "MemberSession ending")

    // With all the channel sessions stopped, we can safely close their outlet, causing a close-cascade.
    if ms.msgOutbox != nil {
        close(ms.msgOutbox)
    }
}

// OpenMsgPipe blocks until the client closes their pipe or this Member session is closing.
//
// WARNING: a client can create multiple pipes, so ensure that all activity is threadsafe.
func (ms *MemberSession) OpenMsgPipe(inPipe Repo_OpenMsgPipeServer) error {

    ms.CtxGo(func() {
        for {
            msg, err := inPipe.Recv()

            if msg != nil {
                chSessID := ChSessID(msg.ChSessID)

                handled := ms.handleCommonMsgs(msg)
                if ! handled {
                    if chSessID == 0 {
                        handled = ms.handleSess0(msg)
                    } else {
                        ms.ChSessionsMutex.RLock()
                        cs := ms.ChSessions[chSessID]
                        ms.ChSessionsMutex.RUnlock()

                        if cs != nil {
                            handled = true
                            if cs.CtxRunning() {
                                cs.msgInbox <- msg
                            }
                        }  
                    }
                }

                if ! handled {
                    ms.Warnf("Unhandled msg to chSessID %d, Op %d", chSessID, msg.Op)
                }
            }

            if err != nil {
                if inPipe.Context().Err() != nil {
                    break
                } else {
                    ms.Warn("OpenMsgPipe pipe recv err: ", err)
                }
            }
        }
    })

    // If the session is stopping, then existing this function will cause inPipe to cancel.
    // If inPipe is closed (from the client side), we want the member session to keep going.
    select {
        case <-ms.CtxStopping():
        case <-inPipe.Context().Done():
    }
    inPipe.SendAndClose(&plan.Status{})

    // Once we return, inPipe is cancelled. 
    return nil
}

func (ms *MemberSession) handleSess0(msg *Msg) bool {

    handled := true

    switch msg.Op {

        case MsgOp_START_CH_SESSION: {
            cs, err := ms.CR.chMgr.StartChannelSession(ms, plan.ChID(msg.BUF0))
            if err != nil {
                ms.Infof(1, "channel session failed to start: %v", err)

                msg.Error = err.Error()
            } else {

                ms.CtxAddChild(cs, nil)

                cs.MemberSession.Infof(1, "channel session opened on ch %v (ChSessID %d)", cs.Agent.Store().ChID().SuffixStr(), cs.ChSessID)

                ms.ChSessionsMutex.Lock()
                ms.ChSessions[cs.ChSessID] = cs
                ms.ChSessionsMutex.Unlock()

                // This tells the client what ch sess the ch session is on
                msg.ChSessID = uint32(cs.ChSessID)
            }

            ms.msgOutbox <- msg
        }

        case MsgOp_RETAIN_COMMUNITY_KEYS:
            //ms.retainCommunityKeysUpto = msg.T0
        case MsgOp_ADD_COMMUNITY_KEYS:
            _, err := ms.commCrypto.Keys.DoCryptOp(&ski.CryptOpArgs{
                CryptOp: ski.CryptOp_IMPORT_USING_PW,
                BufIn: msg.BUF0,
                PeerKey: ms.SessionToken,
            })
            if err != nil {
                ms.Warn("ADD_COMMUNITY_KEYS import error: ", err) 
            } else {
                ms.CR.spSyncActivate()
            }

        default:
            handled = false
    }

    return handled
}



func (ms *MemberSession) handleCommonMsgs(msg *Msg) bool {

    handled := true

    switch msg.Op {

        case MsgOp_COMMIT_TXNS: {
            N := uint32(len(msg.ITEMS))
            txnSet := pdi.NewTxnSet(N)
            var err error
            for i, rawTxn := range msg.ITEMS {
                txnSet.DecodeAndMergeTxn(ms.txnDecoder, rawTxn)
                txn := pdi.NewDecodedTxn(rawTxn)
                err = txn.DecodeRawTxn(ms.txnDecoder)
                if err != nil {
                    break
                }
                txnSet.Segs[i] = txn
            }

            msg.Op = MsgOp_COMMIT_TXNS_COMPLETE
            msg.ITEMS = nil
            msg.EntryInfo = nil
            msg.EntryState = nil
            msg.BUF0 = msg.BUF0[:0]

            onMergeComplete := func(entry *chEntry, ch ChAgent, inErr error) {
                if entry != nil {
                    msg.EntryInfo = entry.Info.Clone()
                    msg.EntryState = entry.State.Clone()
                }
                if inErr != nil {
                    msg.Error = inErr.Error()
                }
                ms.msgOutbox <- msg
            }

            if err == nil {
                txnSet.NewlyAuthored = true

                // With the merge complete, send a masg back to the client saying so, along w/ the entry info
                entry := NewChEntry(entryWasAuthored)
                entry.PayloadTxnSet = txnSet
                entry.onMergeComplete = onMergeComplete

                ms.CR.entriesToMerge <- entry
            } else {
                onMergeComplete(nil, nil, err)
            }
        }

        default:
            handled = false
    }

    return handled
}
