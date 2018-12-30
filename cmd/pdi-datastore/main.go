package main


import (
    "flag"
    log "github.com/sirupsen/logrus"

    "bufio"
    "os"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    //"github.com/plan-systems/go-plan/plan"

)




func main() {

    basePath    := flag.String( "datadir",      "",         "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh datastore" )
    create      := flag.String( "create",       "",         "Specifies a Datastore implementation to be created" )

    flag.Parse()

    {
        sn := NewSnode(basePath)

        err := sn.ReadConfig(*init)
        if err != nil {
            log.WithError(err).Fatalf("failed to read node config")
        }

        if len(*create) > 0 {
            info := &ds.StorageInfo{
                ImplName: *create,
            }
            err = sn.CreateNewStore(info)
            if err != nil {
                log.WithError(err).Fatalf("failed to create '%s' datastore", *create)
            } else {
                sn.WriteConfig()
            }
        } else {

            err = sn.Startup()
            if err != nil {
                log.WithError(err).Fatalf("failed to startup node")
            }

            log.Print("StartServer()")

            sn.StartServer()

            log.Print("StartService() COMPLETE")

            {   
                reader := bufio.NewReader(os.Stdin)
                reader.ReadString('\n')

                //fmt.Printf("Input Char Is : %v", string([]byte(input)[0]))
            }

            sn.Shutdown()
        }
    }

    log.Print("Ending...")
}


