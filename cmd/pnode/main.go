
package main

import (
    "os"
    "flag"
    "log"

    "github.com/plan-tools/go-plan/pnode"

)




func main() {

    basePath    := flag.String( "datadir",      "",         "Directory for config files, keystore, and community repos" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh pnode" )

    flag.Parse()

    params := pnode.PnodeParams{
    }

    if basePath != nil {
        params.BasePath = *basePath
    } else {
        params.BasePath = os.Getenv("HOME") + "/PLAN/pnode/"
    }

    pn := pnode.NewPnode(params)

    err := pn.Startup(*init)
    if err != nil {
        log.Fatalf("pnode.Startup() failed: %v", err)
    }

    // Until there's a util to make a new community....
    if false {
        pn.CreateNewCommunity( "PLAN Foundation" )
    }

    pn.Run()
}


