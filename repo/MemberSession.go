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

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

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


// MemberSessMgr contains functionality that manages live active member connections.  
type MemberSessMgr struct {
    sync.RWMutex

    CR                      *CommunityRepo

    List                    []*MemberSession

}

// Shutdown calls Shutdown() on each MemberSession and blocks until all sessions have completed shutting down.
func (mgr *MemberSessMgr) Shutdown(
    inReason string,
    inBlocker *sync.WaitGroup,
) {

    activeSessions := sync.WaitGroup{}

    for {
        mgr.Lock()
        N := len(mgr.List)
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



// StartSession sets up a MemberSession for use
//
// TODO: close inSKI if an error is returned
func (mgr *MemberSessMgr) StartSession(
    inSessReq *MemberSessionReq,
    inMsgOutlet Repo_OpenMemberSessionServer,
    inBasePath string,
) (*MemberSession, error) {

    // TODO: close inSKI if an error is returned
    var err error

    if len(inSessReq.WorkstationID) > 0 && len(inSessReq.WorkstationID) != plan.WorkstationIDSz {
        return nil, plan.Error(nil, plan.AssertFailed, "invalid workstation ID")
    }

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

    ms := &MemberSession{
        sessMgr: mgr,
        WorkstationID: inSessReq.WorkstationID,
        MemberEpoch: *inSessReq.MemberEpoch,
        SessionToken: make([]byte, 18),
        msgOutlet: inMsgOutlet,
        txnDecoder: ds.NewTxnDecoder(true),
        commCrypto: &CommunityCrypto{

        },
    }

    crypto_rand.Read(ms.SessionToken)

    ms.SetLogLabel(fmt.Sprintf("%s member %d", path.Base(inBasePath), inSessReq.MemberEpoch.MemberID))
    ms.MemberIDStr = ms.MemberEpoch.FormMemberStrID()
    ms.SharedPath = path.Join(inBasePath, ms.MemberIDStr)

    if len(inSessReq.WorkstationID) == 0 {
        ms.WorkstationPath = ms.SharedPath
    } else {
        ms.WorkstationPath = path.Join(ms.SharedPath, plan.Base64p.EncodeToString(ms.WorkstationID[:15]))
    }

    ms.SetLogLabel(fmt.Sprintf("MemSess_%v", ms.MemberIDStr))
    
    err = ms.CtxStart(
        inMsgOutlet.Context(),
        ms.onInternalStartup,
        ms.onInternalShutdown,
        nil,
    )

    if err != nil {
        return nil, err
    }

    mgr.Lock()
    mgr.List = append(mgr.List, ms)
    mgr.Unlock()

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
    plan.Context

    sessMgr         *MemberSessMgr

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



func (ms *MemberSession) onInternalStartup() error {

    ms.ChSessions = make(map[ChSessID]*ChSession)

    var err error
    if err = os.MkdirAll(ms.WorkstationPath, plan.DefaultFileMode); err != nil {
        return err
    }

    // Create a heap-only key hive used for the community keyring
    if ms.commCrypto.Keys, err = hive.StartSession("", "", nil); err != nil {
        return err
    }

    ms.sessMgr.CR.registerCommCrypto(ms.commCrypto)

    //
    // 
    // outbound client msg sender
    //
    ms.msgOutbox = make(chan *Msg, 8)
    ms.CtxGo(func(*plan.Context) {

        for msg := range ms.msgOutbox {

            //ms.Infof(1, "msgOutlet -> (sess %d, ID %d, %s)", msg.ChSessID, msg.ID, MsgOp_name[int32(msg.Op)])

            err := ms.msgOutlet.Send(msg)
            if err != nil {
                ms.Info(1, "msgOutlet error: ", err)
                // TODO

                err = ms.msgOutlet.Context().Err()
                if err != nil {
                    ms.CtxInitiateStop(err.Error())
                }
            }
        }

        ms.sessMgr.detachSession(ms)
        ms.sessMgr.CR.unregisterCommCrypto(ms.commCrypto)
    })

    return nil
}



func (ms *MemberSession) onInternalShutdown() {

    // Shutdown all channel sessions
    {
        waiter := sync.WaitGroup{}

        // First, cause end all client ch sessions
        ms.ChSessionsMutex.RLock()
        waiter.Add(len(ms.ChSessions))
        for _, cs := range ms.ChSessions {
            go func(cs *ChSession) {
                cs.CtxStop(ms.CtxStopReason())
                waiter.Done()
            }(cs)
        }
        ms.ChSessionsMutex.RUnlock()

        waiter.Wait()
    }    

    // With all the channel sessions stopped, we can safely close their outlet, causing a close-cascade.
    if ms.msgOutbox != nil {
        close(ms.msgOutbox)
    }
}

func (ms *MemberSession) detachChSession(cs *ChSession) {
    ms.ChSessionsMutex.Lock()
    delete(ms.ChSessions, cs.ChSessID)
    ms.ChSessionsMutex.Unlock()
}

// EndSession shutsdown this MemberSession, blocking until the session has been completely removed from use.
func (ms *MemberSession) EndSession(inReason string) {
    ms.CtxStop(inReason)
}

// StartChannelSession instantiates a nre channel session for the given channel ID (and accompanying params)
func (ms *MemberSession) StartChannelSession(
    inInvocation *ChInvocation, 
) (*ChSession, error) {

    // If this member session is shutting down, this will return an error (and prevent new sessions from starting)
    err := ms.CtxStatus()
    if err != nil {
        return nil, err
    }

    cs, err := ms.sessMgr.CR.chMgr.StartChannelSession(ms, inInvocation)
    if err != nil {
        ms.Infof(1, "channel session failed to start: %v", err)
        return nil, err
    }

    cs.MemberSession.Infof(1, "channel session opened on ch %v (ChSessID %d)", cs.Agent.Store().ChID().SuffixStr(), cs.ChSessID)

    ms.ChSessionsMutex.Lock()
    ms.ChSessions[cs.ChSessID] = cs
    ms.ChSessionsMutex.Unlock()

    return cs, nil
}




// OpenMsgPipe blocks until the client closes their pipe or this Member session is closing.
//
// WARNING: a client can create multiple pipes, so ensure that all activity is threadsafe.
func (ms *MemberSession) OpenMsgPipe(inPipe Repo_OpenMsgPipeServer) error {

    for ms.CtxRunning() {
        msg, err := inPipe.Recv()

        if msg != nil {

            chSessID := ChSessID(msg.ChSessID)

            if ! ms.handleTopLevelMsgs(msg) {
                ms.ChSessionsMutex.RLock()
                cs := ms.ChSessions[chSessID]
                ms.ChSessionsMutex.RUnlock()

                if cs.CtxRunning() {
                    cs.msgInbox <- msg
                }
            }
        }

        if err != nil {
            return err
        }
    }

    return nil
}

func (ms *MemberSession) handleTopLevelMsgs(msg *Msg) bool {

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

            msg.Op = MsgOp_COMMIT_COMPLETE
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

                ms.sessMgr.CR.entriesToMerge <- entry
            } else {
                onMergeComplete(nil, nil, err)
            }
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
                ms.sessMgr.CR.spSyncActivate()
            }

        default:
            handled = false

    }

    return handled
}
