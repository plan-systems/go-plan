package main

import (
	"github.com/plan-systems/go-plan/pdi"
    "context"
    "flag"
    "os"
    "path"
    "io/ioutil"
    "encoding/json"
    crand "crypto/rand"

    log "github.com/sirupsen/logrus"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    "github.com/plan-systems/go-plan/pcore"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/repo"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/ski/Providers/hive"
)


func main() {

    basePath    := flag.String( "path",         "",                 "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,              "Initializes <datadir> as a fresh datastore" )
    genesisFile := flag.String( "genesis",      "",                 "Creates a new store using the given community genesis file" )

    flag.Parse()

    sn, err := NewSnode(basePath, *init)
    if err != nil {
        log.WithError(err).Fatalf("sn.NewSnode failed")
    }

    if *init {
        log.Info("init successful")
    } else {  
        if len(*genesisFile) > 0 {

            CG, err := loadGenesisInfo(*genesisFile)
            if err != nil {
                log.WithError(err).Fatalf("error loading genesis file %s", *genesisFile)
            }

            err = CG.CreateNewCommunity(sn)
            if err != nil {
                log.WithError(err).Fatalf("failed to create datastore: %s", CG.GenesisSeed.CommunityEpoch.CommunityName)
            }
        }

        if err == nil {
        
            log.Infof("to stop service: kill -s SIGINT %d\n", os.Getpid())

            intr, intrCtx := plan.SetupInterruptHandler(context.Background())
            defer intr.Close()

            snCtx, err := sn.Startup(intrCtx)
            if err != nil {
                log.WithError(err).Fatalf("failed to startup node")
            } else {

                select {
                    case <-snCtx.Done():
                }

                sn.Shutdown("task complete")
            }     
        }
    }

}






func loadGenesisInfo(inPathname string) (*CommunityGenesis, error) {

    params := &GenesisParams{}

    buf, err := ioutil.ReadFile(inPathname)
    if err == nil {
        err = json.Unmarshal(buf, params)
    } else {
        ///err = nil
        //params.CommunityName = "yoyo"
    }

    if err != nil {
        return nil, err
    }

    if len(params.CommunityName) < 3 {
        return nil, plan.Error(nil, plan.AssertFailed, "missing valid community name")
    }

    needed := plan.CommunityIDSz - len(params.CommunityID)
    if needed < 0 {
        params.CommunityID = params.CommunityID[:plan.CommunityIDSz]
    } else if needed > 0 {
        remain := make([]byte, needed)
        crand.Read(remain)
        params.CommunityID = append(params.CommunityID, remain...)
    }

    genesis := &CommunityGenesis{
        GenesisSeed: repo.GenesisSeed{
            CommunityEpoch:  &pdi.CommunityEpoch{
                CommunityID: params.CommunityID,
                CommunityName: params.CommunityName,
                EntryHashKit: ski.HashKitID_LegacyKeccak_256,
                //EpochTID: plan.GenerateTID(nil),
                MaxMemberClockDelta: 120,
            },
        },
    }

/*
    // The genesis community epoch has a randomly generated ID (vs one set from the originating entry hash).
    // Otherwise, there'd be no way for the genesis entry to be decrypted (since EntryCrypt must specify the epoch ID).
    randTID := plan.TIDBlob{}
    crand.Read(randTID[:])
    genesis.GenesisSeed.CommunityEpoch.EpochTID = make([]byte, plan.TIDSz)
    plan.TID(genesis.GenesisSeed.CommunityEpoch.EpochTID).SetTimeAndHash(plan.NowFS(), randTID[:])
*/
    genesis.MemberSessions.Host = genesis

    return genesis, nil
}




// CommunityGenesis is a helper for creating a new community
type CommunityGenesis struct {

    GenesisSeed         repo.GenesisSeed
    MemberSeed          repo.MemberSeed


    MemberSessions      repo.MemberSessions
    txnsToCommit        []pdi.RawTxn
}



/*****************************************************
** MemberHost (interface)
**/

// Context -- see interface MemberHost
func (CG *CommunityGenesis) Context() context.Context {
    return context.Background()
}

// CommitEntryTxns -- see interface MemberHost
func (CG *CommunityGenesis) CommitEntryTxns(inEntryURID []byte, inTxns []pdi.RawTxn) {
    CG.txnsToCommit = append(CG.txnsToCommit, inTxns...)
}

// LatestCommunityEpoch -- see interface MemberHost
func (CG *CommunityGenesis) LatestCommunityEpoch() *pdi.CommunityEpoch {
    return CG.GenesisSeed.CommunityEpoch
}

// LatestStorageEpoch -- see interface MemberHost
func (CG *CommunityGenesis) LatestStorageEpoch() pdi.StorageEpoch {
    return *CG.GenesisSeed.StorageEpoch
}

// OnSessionEnded -- see interface MemberHost
func (CG *CommunityGenesis) OnSessionEnded(inSession *repo.MemberSession) {
    CG.MemberSessions.OnSessionEnded(inSession)
}






// CreateNewCommunity creates a new community.
//
// Pre: CommunityEpoch is already set up
func (CG *CommunityGenesis) CreateNewCommunity(
    sn *Snode,
) error {

    keyDir, err := hive.GetSharedKeyDir()
    if err != nil { return err }



    CG.MemberSeed = repo.MemberSeed{
        RepoSeed: &repo.RepoSeed{
            Services: []*plan.ServiceInfo{
                &plan.ServiceInfo{
                    Addr:    sn.Config.GrpcNetworkAddr,
                    Network: sn.Config.GrpcNetworkName,
                },
            },  
        },
        MemberEpoch: &pdi.MemberEpoch{
            MemberID: plan.GenesisMemberID, // TODO: randomize?  What ensures Proof of Independence Assurance?
        },
    }

    genesisSKI, err := hive.StartSession(
        keyDir,
        CG.MemberSeed.MemberEpoch.FormMemberStrID(),
        nil,
    )
    if err != nil { return err }
    defer genesisSKI.EndSession("session complete")

    // Generate a new storage epoch
    if err == nil {
        CG.GenesisSeed.StorageEpoch, err = ds.NewStorageEpoch(
            genesisSKI,
            CG.GenesisSeed.CommunityEpoch,
        )
        
        // Generate random channel IDs for the community's global/shared channels
        crand.Read(CG.GenesisSeed.StorageEpoch.CommunityChIDs)
    }

   // Generate the first community key  :)
    if err == nil {
        CG.GenesisSeed.CommunityEpoch.KeyInfo, err = ski.GenerateNewKey(
            genesisSKI,
            CG.GenesisSeed.CommunityEpoch.CommunityKeyringName(),
            ski.KeyInfo{
                KeyType: ski.KeyType_SymmetricKey,
                CryptoKit: ski.CryptoKitID_NaCl,
            },
        )
    }  

    // Generate new member private keys
    if err == nil {
        err = CG.MemberSeed.MemberEpoch.RegenMemberKeys(genesisSKI, CG.GenesisSeed.CommunityEpoch)
    }

    // Generate the genesis storage addr
    var genesisAddr []byte
    if err == nil {
        genesisAddr, err = CG.GenesisSeed.StorageEpoch.GenerateNewAddr(genesisSKI)
    }

    // Emit all the genesis entries
    var ms *repo.MemberSession
    if err == nil {
        ms, err = CG.MemberSessions.StartSession(
            &repo.SessionReq{
                CommunityID: CG.GenesisSeed.CommunityEpoch.CommunityID,
                MemberEpoch: CG.MemberSeed.MemberEpoch,
            },
            genesisSKI,
            path.Join(sn.BasePath, "genesis"),
        )
        if err == nil {
            err = CG.emitGenesisEntries(ms)
        }
    }

    if err == nil {
        deposits := []*pdi.Transfer{
            &pdi.Transfer{
                To: genesisAddr,
                Kb:  1 << 40,
                Ops: 1 << 40,
            },
        }

        err = sn.CreateNewStore(
            "badger", 
            deposits,
            CG.txnsToCommit,
            *CG.GenesisSeed.StorageEpoch,
        )
    }

    CG.MemberSessions.Shutdown("genesis complete", nil)


    // Write out the MemberSeed file
    if err == nil {

        packer := ski.NewPacker(false)
        err = packer.ResetSession(
            genesisSKI,
            ski.KeyRef{
                KeyringName: CG.GenesisSeed.CommunityEpoch.FormGenesisKeyringName(),
            }, 
            CG.GenesisSeed.CommunityEpoch.EntryHashKit,
            nil,
        )

        buf, _ := CG.GenesisSeed.Marshal()

        // Pack and sign the genesis seed
        if err == nil { 

            var packingInfo ski.PackingInfo
            err = packer.PackAndSign(0, buf, nil, 0, &packingInfo)

            CG.MemberSeed.RepoSeed.SignedGenesisSeed = packingInfo.SignedBuf
            CG.MemberSeed.RepoSeed.SuggestedDirName = CG.GenesisSeed.StorageEpoch.FormSuggestedDirName()

            // Write out the final MemberSeed file woohoo
            if err == nil { 
                buf, err = CG.MemberSeed.Marshal()

                // TODO: encrypt this and put keys in it    
                err = ioutil.WriteFile(CG.GenesisSeed.CommunityEpoch.CommunityName + ".plan.seed", buf, plan.DefaultFileMode)
            }
        }
    }


    return err
}



type chEntry struct {
    Info            pdi.EntryInfo
    Body            []byte

    whitelist       bool    
    chEpoch         *pdi.ChannelEpoch
    body            pcore.Marshaller
    chID            pdi.CommunityChID
    parentEntry     *chEntry
}


func (CG *CommunityGenesis) emitGenesisEntries(ms *repo.MemberSession) error {

    //curTime := CG.GenesisSeed.CommunityEpoch.TimeStarted

    //chEpochs := 


    genesisID := uint32(CG.MemberSeed.MemberEpoch.MemberID)

    newACC := &chEntry{
        whitelist: true,
        chID: pdi.CommunityChID_RootACC,
        chEpoch: &pdi.ChannelEpoch{
            ChProtocol: repo.ChProtocolACC,
            DefaultAccessLevel: pdi.AccessLevel_READ_ACCESS,
            AccessLevels: map[uint32]pdi.AccessLevel{
                genesisID: pdi.AccessLevel_ADMIN_ACCESS,
            },
        },
    }

    newMemberReg := &chEntry{
        whitelist: true,
        chID: pdi.CommunityChID_MemberRegistry,
        chEpoch: &pdi.ChannelEpoch{
            ChProtocol: repo.ChProtocolMemberRegistry,
        },
    }

    postMember := &chEntry{
        whitelist: true,
        chID: pdi.CommunityChID_MemberRegistry,
        body: CG.MemberSeed.MemberEpoch,
        parentEntry: newMemberReg,
    }

    newEpochHistory := &chEntry{
        whitelist: true,
        chID: pdi.CommunityChID_EpochHistory,
        chEpoch: &pdi.ChannelEpoch{
            ChProtocol: repo.ChProtocolCommunityEpochs,
        },
    }

    postGenesisEpoch := &chEntry{
        whitelist: true,
        chID: pdi.CommunityChID_EpochHistory,
        body: CG.GenesisSeed.CommunityEpoch,
        parentEntry: newEpochHistory,
    }


    entries := []*chEntry{
        newACC,
        newMemberReg,
        postMember,
        newEpochHistory,
        postGenesisEpoch,
    }
    

    nowFS := plan.NowFS()

    for _, entry := range entries {

        entry.Info.TIDs = make([]byte, pdi.EntryTID_NormalNumTIDs * plan.TIDSz)
        entry.Info.AuthorMemberID = genesisID
        entry.Info.EntryID().SetTimeFS(nowFS)

        if ! entry.whitelist {
            copy(entry.Info.GetTID(pdi.EntryTID_AuthorEntryID),         postMember.Info.EntryID())
            copy(entry.Info.GetTID(pdi.EntryTID_ACCEntryID),            newACC.Info.EntryID())

            if entry.parentEntry != nil {
                copy(entry.Info.GetTID(pdi.EntryTID_ChannelEpochEntryID),   entry.parentEntry.Info.EntryID())
            }
        }

        var body []byte

        if entry.chEpoch != nil {
            entry.Info.EntryOp = pdi.EntryOp_NEW_CHANNEL_EPOCH

            body, _ = entry.chEpoch.Marshal()
        } else {
            entry.Info.EntryOp = pdi.EntryOp_POST_CONTENT

            body, _ = entry.body.Marshal()
        }

        // Set the dest channel ID (this will be 0s for community channels not yet created)
        entry.Info.ChannelID = CG.GenesisSeed.StorageEpoch.CommunityChID(entry.chID)

        txns, entryURID, err := ms.EncryptAndEncodeEntry(&entry.Info, body)
        if err != nil {
            return err
        }

        // Set the channel IDs of the newly generated community channels
        if entry.chEpoch != nil {
            CG.GenesisSeed.StorageEpoch.CommunityChID(entry.chID).AssignFromTID(entry.Info.EntryID())
        }

        if entry.whitelist {
            CG.GenesisSeed.StorageEpoch.GenesisURIDs = append(CG.GenesisSeed.StorageEpoch.GenesisURIDs, entryURID)
        }

        CG.CommitEntryTxns(entryURID, txns)

    }

    // Set the genesis community epoch ID now that the entry ID has been generated
    CG.GenesisSeed.CommunityEpoch.EpochTID = postGenesisEpoch.Info.EntryID()

    return nil
}
