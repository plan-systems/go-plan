package main

import (
	"github.com/plan-systems/go-plan/pdi"
    "context"
    "flag"
    "os"
    "io/ioutil"
    "encoding/json"
    crand "crypto/rand"

    log "github.com/sirupsen/logrus"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/ski/Providers/hive"
)



func main() {

    basePath    := flag.String( "path",         "",                 "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,              "Initializes <datadir> as a fresh datastore" )
    genesis     := flag.String( "genesis",      "",                 "Creates a new store using the given community genesis file" )

    flag.Parse()

    sn, err := NewSnode(basePath, *init)
    if err != nil {
        log.WithError(err).Fatalf("sn.NewSnode failed")
    }

    switch {

        case *init == true:
            log.Info("init successful")
            
        case len(*genesis) > 0: {

            info, err := loadGenesisInfo(*genesis)
            if err != nil {
                log.WithError(err).Fatalf("error loading genesis file %s", *genesis)
            }

            err = createNewCommunity(sn, info)
            if err != nil {
                log.WithError(err).Fatalf("failed to create datastore: %s", info.CommunityName)
            }
        } 
        
        default: {
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




func loadGenesisInfo(inPathname string) (*pdi.CommunityEpoch, error) {

    params := &GenesisParams{}

    buf, err := ioutil.ReadFile(inPathname)
    if err == nil {
        err = json.Unmarshal(buf, params)
    }

    if err != nil {
        //return nil, err
        params.CommunityName = "Drew2"
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

    commEpoch := &pdi.CommunityEpoch{
        CommunityID: params.CommunityID,
        CommunityName: params.CommunityName,
        EntryHashKit: ski.HashKitID_LegacyKeccak_256,
        TimeStarted: plan.Now().UnixSecs,
    }

    return commEpoch, nil
}


func createNewCommunity(
    sn *Snode,
    inCommunityEpoch *pdi.CommunityEpoch,
) error {

    keyDir, err := hive.GetSharedKeyDir()
    if err != nil { return err }

    memberEpoch := &pdi.MemberEpoch{}
    memberEpoch.GenerateNewMemberID()

    genesisSKI, err := hive.StartSession(
        keyDir,
        memberEpoch.FormMemberStrID(),
        nil,
    )
    defer genesisSKI.EndSession("done")
    if err != nil { return err }

   // Generate the first community key  :)
    inCommunityEpoch.KeyInfo, err = ski.GenerateNewKey(
        genesisSKI,
        inCommunityEpoch.CommunityKeyringName(),
        ski.KeyInfo{
            KeyType: ski.KeyType_SymmetricKey,
            CryptoKit: ski.CryptoKitID_NaCl,
        },
    )
    if err != nil { return err }

  
    // Generate the genesis member keys
    err = memberEpoch.RegenMemberKeys(genesisSKI, inCommunityEpoch)
    if err != nil { return err }

    stEpoch, err := ds.NewStorageEpoch(
        genesisSKI,
        *inCommunityEpoch,
    )
    if err != nil { return err }


    {
        seed := pdi.MemberSeed{
            RepoSeed: &pdi.RepoSeed{
                CommunityEpoch: inCommunityEpoch,
                StorageEpoch: stEpoch,
                Services: []*pdi.ServiceInfo{
                    &pdi.ServiceInfo{
                        Addr:    sn.Config.GrpcNetworkAddr,
                        Network: sn.Config.GrpcNetworkName,
                    },
                },
            },
            MemberEpoch: memberEpoch,
        }
        buf, err := seed.Marshal()
        if err != nil { return err }

        // TODO: encrypt this and put keys in it    
        err = ioutil.WriteFile(inCommunityEpoch.CommunityName + ".plan.seed", buf, plan.DefaultFileMode)
        if err != nil { return err }
    }

    // Generate the genesis storage addr
    genesisAddr, err := stEpoch.GenerateNewAddr(genesisSKI)
    if err != nil { return err }

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
        *stEpoch,
    )



    return nil
}

