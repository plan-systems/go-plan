package main

import (
	"github.com/plan-systems/go-plan/pdi"
    "context"
    "flag"
    log "github.com/sirupsen/logrus"

    //"bufio"
    //"os"
    "crypto/rand"
    //"time"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/ski/Providers/hive"
)

func main() {

    basePath    := flag.String( "datadir",      "",                 "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,              "Initializes <datadir> as a fresh datastore" )
    create      := flag.String( "create",       "",                 "Creates a new store with the given community name" )
    dsImplName  := flag.String( "datastore",    ds.DefaultImplName, "Specifies a Datastore implementation to be created (e.g. badger)" )

    flag.Parse()

    ctx := context.Background()
    intrh, ctx := plan.SetupInterruptHandler(ctx)
	defer intrh.Close()

    {
        sn, err := NewSnode(basePath)
        if err != nil {
            log.WithError(err).Fatalf("sn.NewSnode failed")
        }

        err = sn.ReadConfig(*init)
        if err != nil {
            log.WithError(err).Fatalf("sn.ReadConfig faile")
        }

        switch {

        case *init == true:
            // No op
            
        case len(*create) > 0: {

            epoch := pdi.StorageEpoch{
                CommunityName: *create,
                CommunityID: make([]byte, plan.CommunityIDSz),
                Salt: make([]byte, 32),
                StartTime: plan.Now().UnixSecs,
                FuelPerKb: 1,
                FuelPerTxn: 10,
            }
            rand.Read(epoch.Salt)
            rand.Read(epoch.CommunityID)

            // Generate pub/private key of genesis acct
            skiTool, err := ski.NewSessionTool(
                hive.NewProvider(),
                "genesis-admin",
                epoch.CommunityID,
            )
            if err != nil {
                log.Fatal(err)
            }


            // Create the admin acct
            decoder := ds.NewTxnDecoder()
            encoder, err := ds.NewTxnEncoder(0)
            err = encoder.ResetSession(
                decoder.TxnEncoderInvocation(),
                skiTool.Session,
                epoch.CommunityID,
            )
            if err != nil {
                log.Fatal(err)
            }

            var genesisAddr *ski.PubKey
            genesisAddr, err = encoder.GenerateNewAccount()
            if err != nil {
                log.Fatal(err)
            }

            deposits := []*pdi.Transfer{
                &pdi.Transfer{
                    To: genesisAddr.Bytes,
                    Fuel: 10000000,
                    Mana: 1000,
                },
            }
  
            epoch.GenesisID = genesisAddr.Bytes

            err = sn.CreateNewStore(
                *dsImplName, 
                deposits,
                epoch,
            )
            if err != nil {
                log.WithError(err).Fatalf("failed to create datastore: %s", *create)
            }

            skiTool.EndSession("donezo")
            
        } 
        
        default: {

            err := sn.Startup()
            if err != nil {
                log.WithError(err).Fatalf("failed to startup node")
            } else {

                sn.StartServer()

                log.Info("RUNNING")

                select {
                    case <-ctx.Done():
                }
            }
            
        } }

        sn.Shutdown()
    }

    
    log.Print("Ending...")
}






/*

type GenesisTool struct {
    SkiSession      ski.Session
    TxnDecoder      TxnDecoder
    TxnEncoder      TxnEncoder
}


func (genesis *GenesisTool) 



   if true {

        // Generate pub/private key of genesis acct
        st, err := ski.NewSessionTool(
            filesys.Provider.InvocationStr(),
            "genesis-admin",
            nil,
        )
        if err != nil {
            log.Fatal(err)
        }

        // Create the admin acct
        decoder := ds.NewTxnDecoder()
        encoder, err := ds.NewTxnEncoder(0)
        err = encoder.ResetSession(
            decoder.TxnEncoderInvocation(),
            st.Session,
            st.CommunityID,
        )
        if err != nil {
            log.Fatal(err)
        }

        pubKey, err := encoder.GenerateNewAccount()
        if err != nil {
            log.Fatal(err)
        }

        // Create/Write genesis json file. 
        genesis := plan.CommunityEpoch {
            

        }


        // Create a new storage provider, passing it the genesis json (so it can perform account bootstrapping)


        // With a StorageProvider bootstrapped (and the genesis admin acct allocated), bootstrap the community's reserved channels.
        // To the SP, this just means committing a pile of new txns
        

        // Transfer power from the genesis acct to the community admins, halt the genesis acct, update the genesis completion time (ensure genesis period is immutable)

    }
*/