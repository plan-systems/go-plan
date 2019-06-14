
package main

import (
    //"fmt"
    "flag"
    "os"

    "log"
    //"io"
    //"bytes"
    //"fmt"
	//"os/user"
    //"path"
    //"crypto/md5"
    //"encoding/hex"

    "github.com/plan-systems/go-plan/plan"
    //"github.com/plan-systems/go-plan/ski"
    //"github.com/plan-systems/go-plan/pdi"
    //"github.com/plan-systems/go-plan/pservice"

    "context"
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
        intr, intrCtx := plan.SetupInterruptHandler(context.Background())
        defer intr.Close()

        //intrCtx, _ := context.WithTimeout(context.Background(), 30 * time.Second)
        
        err := pn.Startup(intrCtx)
        if err != nil {
            pn.Fatalf("failed to startup repo node")
        } else {
            pn.Infof(0, "to stop: kill -s SIGINT %d", os.Getpid())

            select {
                case <- pn.Ctx.Done():
            }

            pn.CtxStop("pnode complete")
        }
    }
}
