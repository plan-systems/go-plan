
package main

import (
    //"fmt"
    "flag"
    //"os"
    //"time"
    "log"
    //"io"
    //"bytes"
    //"fmt"
	//"os/user"
    //"path"
    //"crypto/md5"
    //"encoding/hex"

    //"github.com/plan-systems/go-plan/plan"
    //"github.com/plan-systems/go-plan/ski"
    //"github.com/plan-systems/go-plan/pdi"
    //"github.com/plan-systems/go-plan/pservice"
)

func main() {

    basePath    := flag.String( "path",         "",         "Directory for all files associated with this repo" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh repo" )

    flag.Parse()
    flag.Set("logtostderr", "true")
    flag.Set("v", "2")

    pn, err := NewPnode(basePath, *init)
    if err != nil {
        log.Fatal(err)
    }

    {
        err := pn.Startup()
        if err != nil {
            pn.Fatalf("failed to startup pnode")
        } else {
            pn.AttachInterruptHandler()
            pn.CtxWait()
        }
    }
}
