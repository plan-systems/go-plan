package main


import (
    "flag"
    log "github.com/sirupsen/logrus"

    //"github.com/plan-systems/go-plan/plan"

)




func main() {

    basePath    := flag.String( "datadir",      "",         "Directory for all files associated with this datastore" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh datastore" )
    create      := flag.String( "create",       "",         "Specifies a Datastore implementation to be created" )

    flag.Parse()

    sn := NewSnode(basePath)

    err := sn.ReadConfig(*init)
    if err != nil {
        log.WithError(err).Fatalf("failed to read node config")
    }

    if len(*create) > 0 {
        info := &CStorageInfo{
            ImplName: *create,
        }
        err = sn.CreateNewStore(info)
        if err != nil {
            log.WithError(err).Fatalf("failed to create '%s' datastore", *create)
        }
    }

    err = sn.Startup()
    if err != nil {
        log.WithError(err).Fatalf("failed to startup node")
    }

    if create != nil {
        sn.WriteConfig()
    }

    sn.Run()
}


