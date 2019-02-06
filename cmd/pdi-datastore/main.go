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

var datastores = []string{
    "badger",
}

func main() {

    basePath    := flag.String( "datadir",      "",                 "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,              "Initializes <datadir> as a fresh datastore" )
    create      := flag.String( "create",       "",                 "Creates a new store with the given community name" )
    datastore   := flag.String( "datastore",    datastores[0],      "Specifies a Datastore implementation to be created (e.g. badger)" )

    flag.Parse()

    ctx := context.Background()
    intrh, ctx := plan.SetupInterruptHandler(ctx)
	defer intrh.Close()

    {
        sn := NewSnode(basePath)

        {
            err := sn.ReadConfig(*init)
            if err != nil {
                log.WithError(err).Fatalf("failed to read storage node config")
            }
        }

        switch {

        case *init == true:
            // No op
            
        case len(*create) > 0: {

            if datastore == nil {
                datastore = &datastores[0]
            }
            config := &ds.StorageConfig{
                ImplName: *datastore,
                Epoch: plan.CommunityEpoch{
                    CommunityName: *create,
                    CommunityID: make([]byte, plan.CommunityIDSz),
                    StartTime: plan.Now(),
                    GasPerKb: 1,
                    GasTxnBase: 10,
                },
            }
            rand.Read(config.Epoch.CommunityID)

            config.HomePath = plan.MakeFSFriendly(config.Epoch.CommunityName, config.Epoch.CommunityID[:2])

            // Generate pub/private key of genesis acct
            var (
                err error
                st *ski.SessionTool
            )
            st, err = ski.NewSessionTool(
                hive.NewProvider(),
                "genesis-admin",
                config.Epoch.CommunityID,
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

            config.Epoch.GenesisID = pubKey.Bytes

            deposits := []*pdi.Transfer{
                &pdi.Transfer{
                    To: pubKey.Bytes,
                    Gas: 1000000,
                    Fiat: 1000,
                },
            }

            err = sn.CreateNewStore(config, deposits)
            if err != nil {
                log.WithError(err).Fatalf("failed to create '%s' datastore", *create)
            }

            //di.EntryOp_NEW_CHANNEL_EPOCH

            st.EndSession("donezo")
            
        } 
        
        default: {

            err := sn.Startup()
            if err != nil {
                log.WithError(err).Fatalf("failed to startup node")
            }

            log.Print("StartServer()")

            sn.StartServer()

            log.Print("RUNNING")

            select {
                case <-ctx.Done():
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