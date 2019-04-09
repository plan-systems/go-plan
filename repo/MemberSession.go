package repo

import (
	crypto_rand "crypto/rand"
    "os"
    "path"
    //"io/ioutil"
    //"strings"
    "sync"
    "time"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    "context"
    "fmt"
    
    //"github.com/plan-systems/go-plan/ski/Providers/hive"


 	//"google.golang.org/grpc"
    //"google.golang.org/grpc/metadata"

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


// MemberSessions contains functionality that manages live active member connections.  
type MemberSessions struct {
    sync.RWMutex

    Host                    MemberHost

    List                    []*MemberSession

    // Used to block/signal when all sessions are ended/stopped
    SessionsActive          sync.WaitGroup
}

// Shutdown calls Shutdown() on each MemberSession and blocks until all sessions have completed shutting down.
func (MS *MemberSessions) Shutdown(
    inReason string,
    inBlocker *sync.WaitGroup,
) {

    MS.RLock()
    for _, ms := range MS.List {
        go ms.EndSession(inReason)
    }
    MS.RUnlock()

    MS.SessionsActive.Wait()
    if inBlocker != nil {
       inBlocker.Done()
    }
}


// OnSessionEnded is called after a MemberSession has completed shutting down
//
// THREADSAFE
func (MS *MemberSessions) OnSessionEnded(inSess *MemberSession) {

    MS.Lock()
    N := int32(len(MS.List))
    for i, ms := range MS.List {
        if ms == inSess {
            N--
            MS.List[i] = MS.List[N]
            MS.List[N] = nil
            MS.List = MS.List[:N]
            MS.SessionsActive.Done()
            break
        }
    }
    MS.Unlock()

}



// StartSession sets up a MemberSession for use
//
// TODO: close inSKI if an error is returned
func (MS *MemberSessions) StartSession(
    inSessReq *SessionReq,
    inSKI ski.Session,
    inBasePath string,
) (*MemberSession, error) {

    // TODO: close inSKI if an error is returned
    var err error

    if len(inSessReq.WorkstationID) > 0 && len(inSessReq.WorkstationID) != plan.CommunityIDSz {
        return nil, plan.Error(nil, plan.AssertFailed, "invalid workstation ID")
    }

    // Is the session already open?
    {
        var match *MemberSession

        MS.RLock()
        for _, ms := range MS.List {
            if ms.MemberEpoch.MemberID == inSessReq.MemberEpoch.MemberID {
                match = ms
                break 
            }
        }
        MS.RUnlock()

        if match != nil {
            // TODO end/close existing session
            return nil, plan.Error(nil, plan.AssertFailed, "session already open")
        }
    }

    ms := &MemberSession{
        Host: MS.Host,
        PersonalSKI: inSKI,
        WorkstationID: inSessReq.WorkstationID,
        MemberEpoch: *inSessReq.MemberEpoch,
        CommunityEpoch: MS.Host.LatestCommunityEpoch(),
    }

    ms.communityKey = ms.CommunityEpoch.CommunityKeyRef()
    ms.MemberIDStr = ms.MemberEpoch.FormMemberStrID()
    ms.SharedPath = path.Join(inBasePath, ms.MemberIDStr)

    if len(inSessReq.WorkstationID) == 0 {
        ms.WorkstationPath = ms.SharedPath
    } else {
        ms.WorkstationPath = path.Join(ms.SharedPath, plan.Base64.EncodeToString(ms.WorkstationID[:15]))
    }
    
    err = ms.flow.Startup(
        ms.Host.Context(),
        fmt.Sprintf("MemberSession %v", ms.MemberIDStr),
        ms.onInternalStartup,
        ms.onInternalShutdown,
    )

    if err != nil {
        return nil, err
    }

    MS.SessionsActive.Add(1)

    MS.Lock()
    MS.List = append(MS.List, ms)
    MS.Unlock()

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

// MemberHost is a context for a MemberSession to operate wthin.
type MemberHost interface {

    // Returns an encapsulating context
    Context() context.Context

    // Commits the newly authored txn
    CommitAuthoredTxn(inTxn pdi.RawTxn)

    // Returns the latest community epoch
    LatestCommunityEpoch() pdi.CommunityEpoch

    // Returns the latest storage epoch.
    LatestStorageEpoch() pdi.StorageEpoch

    // The given session is ending
    OnSessionEnded(inSession *MemberSession)
}



// MemberSession represents a user/member "logged in", meaning a SKI session is active.
type MemberSession struct {
    flow            plan.Flow

    // The current community epoch
    CommunityEpoch  pdi.CommunityEpoch

    // Allows key callback functionality 
    Host            MemberHost

    // Pathname to member repo files (shared by all client instances)
    SharedPath      string

    // Pathname to member repo files specific to a member client/workstation ID.
    WorkstationPath string

    // Uniquely identifies the client install instance
    WorkstationID   []byte

    // Ah, a contraction?
    // Nay, the MemberClient more long-winded name is the *MemberClientLifetimeSession*
    // Sequence of sigs/token exchanged by Unity client and here, ensuring that there can't be an imposter session w/o
    //    the member from knowing it next login.   
    // Note that both non-time-dependent (TLS cert exchange) and this approach feature a no-password UX (though a device
    //    may have a device pin to nerf anonymous easy physical theft, etc).
    //sessionDB      *badger.DB
    // Host 



    // Outbound entries from channel adapters to be committed to the community's storage provider(s)
    EntriesToCommit chan *entryIP

    MemberEpoch     pdi.MemberEpoch
    MemberIDStr     string

    Packer          ski.PayloadPacker

    TxnEncoder      pdi.TxnEncoder
    PersonalSKI     ski.Session

    //txnSignKey     *ski.KeyRef
    //txnSignKeyInfo  *ski.KeyInfo

    //memSignKey      *ski.KeyRef
    //memSignKeyInfo   *ski.KeyInfo

    //communitySKI    ski.Session // TODO
    communityKey    ski.KeyRef

    ChSessions      sync.WaitGroup



}




func (ms *MemberSession) StartChSession(
    chID          plan.ChannelID,
    chAdapterDesc string,
) (ChSession, error) {


    return ChSession{}, nil
}




func (ms *MemberSession) onInternalStartup() error {

 
    var err error


    if err = os.MkdirAll(ms.WorkstationPath, plan.DefaultFileMode); err != nil {
        return err
    }

    // Set up the entry packer using the singing key associated w/ the member's current MemberEpoch
    ms.Packer = ski.NewPacker(false)
    err = ms.Packer.ResetSession(
        ms.PersonalSKI,
        ski.KeyRef{
            KeyringName: ms.MemberEpoch.FormSigningKeyringName(ms.CommunityEpoch.CommunityID),
            PubKey: ms.MemberEpoch.PubSigningKey,
        },
        ms.CommunityEpoch.EntryHashKit,
        nil,
    )
    if err != nil { return err }

    // Set up the txn encoder
    ms.TxnEncoder = ds.NewTxnEncoder(false, ms.Host.LatestStorageEpoch())

    // Use the member's latest txn/storage signing key.
    if err = ms.TxnEncoder.ResetSigner(ms.PersonalSKI, nil); err != nil { 
        return err
    }

    //
    // 
    // outbound entry processor
    //
    // encrypts and encodes entries from channel sessions, sending the finished txns to the host repo to be stored/saved.
    ms.flow.ShutdownComplete.Add(1)
    ms.EntriesToCommit = make(chan *entryIP, 8)
    go func() {
        for entryIP := range ms.EntriesToCommit {

            txns, err := ms.EncryptAndEncodeEntry(&entryIP.Entry.Info, entryIP.Entry.Body)
            if err != nil {
                ms.flow.FilterFault(err)
                continue
            }

            for _, txn := range txns {
                ms.flow.Log.Infof("encoded    txn %v", ski.BinDesc(txn.URID))

                ms.Host.CommitAuthoredTxn(txn)
            }
        }

        ms.PersonalSKI.EndSession(ms.flow.ShutdownReason)

        ms.flow.ShutdownComplete.Done()
    }()

    return nil
}


func (ms *MemberSession) onInternalShutdown() {

    // TODO: shutdown all channel activity for this member session
    ms.ChSessions.Wait()

    // With all the channel sessions stopped, we can safely close their outlet, causing a close-cascade.
    if ms.EntriesToCommit != nil {
        close(ms.EntriesToCommit)
    }
}



// EndSession shutsdown this MemberSession, blocking until the session has been completely removed from use.
func (ms *MemberSession) EndSession(inReason string) {
    ms.flow.Shutdown(inReason)
    ms.Host.OnSessionEnded(ms)
}





func (ms *MemberSession) CommunityEncrypt(
    inBuf    []byte,
) ([]byte, error) {

    out, err := ms.PersonalSKI.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        OpKey: &ms.communityKey,
        BufIn: inBuf,
    })
    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}

func (ms *MemberSession) ExportCommunityKeyring(
)  (outTome, outPassword []byte) {

    var pw [8]byte
    crypto_rand.Read(pw[:])

    out, err := ms.PersonalSKI.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_EXPORT_USING_PW,
        PeerKey: pw[:],
        TomeIn: &ski.KeyTome{
            Keyrings: []*ski.Keyring{
                &ski.Keyring{
                    Name: ms.communityKey.KeyringName,
                },
            },
        },
    })

    if err != nil {
        return nil, nil
    }

    return out.BufOut, pw[:]
}

/*

func (sess *MemberSession) SignDigest(
    inDigest []byte,
) ([]byte, error) {

    out, err := sess.personalSKI.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_SIGN,
        OpKey: sess.memberKey,
        BufIn: inDigest,
    })
    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}
*/



func (ms *MemberSession) EncryptAndEncodeEntry(
    ioInfo *pdi.EntryInfo,
    inBody []byte,
) ([]pdi.RawTxn, error) {

    var err error

    if ioInfo.TimeAuthored == 0 {
        t := plan.Now()
        ioInfo.TimeAuthored     = t.UnixSecs
        ioInfo.TimeAuthoredFrac = uint32(t.FracSecs)
    }

    // TODO: allow multiple entries to be put into a plan.Block

    entryCrypt := pdi.EntryCrypt{
        CommunityPubKey: ms.communityKey.PubKey,
    }

// TODO: use scrap buf
    headerBuf, err := ioInfo.Marshal()

    // Have the member sign the header
    var packingInfo ski.PackingInfo
    err = ms.Packer.PackAndSign(
        plan.Encoding_Pb_EntryInfo,
        headerBuf,
        inBody,
        0,
        &packingInfo,
    )
    entryCrypt.PackedEntry, err = ms.CommunityEncrypt(packingInfo.SignedBuf)

// TODO: use scrap buf
    entryBuf, err := entryCrypt.Marshal()

    var scrap [pdi.URIDBinarySz]byte
    entryURID := pdi.URIDFromInfo(scrap[:], ioInfo.TimeAuthored, packingInfo.Hash)
    txns, err := ms.TxnEncoder.EncodeToTxns(
        entryBuf,
        entryURID,
        plan.Encoding_Pb_EntryCrypt,
        nil,
        ioInfo.TimeAuthored,
    )

    if err != nil {
        return nil, err
    }
    
    return txns, nil
}




var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

func (ms *MemberSession) GetItWorking() {

    for i := 0; i < 100 && ms.flow.IsRunning(); i++ {

        cheese := fmt.Sprintf("#%d: %s", i, gTestBuf)


        ms.EntriesToCommit <- &entryIP{
            Entry: DecryptedEntry{
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