
package main

import (
    //"fmt"
    "flag"
    "io/ioutil"
    "os"

    log "github.com/sirupsen/logrus"
    //"time"
    //"io"
    //"bytes"
    //"fmt"
	//"os/user"
    //"path"
    //"crypto/md5"
    //"github.com/plan-systems/go-plan/ski/Providers/hive"
	//_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
    //"encoding/hex"

    "github.com/plan-systems/go-plan/repo"

    "github.com/plan-systems/go-plan/plan"
    //"github.com/plan-systems/go-plan/ski"
    //"github.com/plan-systems/go-plan/pdi"
    //"github.com/plan-systems/go-plan/pservice"

    "context"
 	//"google.golang.org/grpc"
    //"google.golang.org/grpc/metadata"

    //ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    //"github.com/plan-systems/go-plan/ski/Providers/filesys"

)


func main() {

    basePath    := flag.String( "path",         "",         "Directory for all files associated with this repo" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh repo" )
    seed        := flag.String( "seed",         "",         "Reads the given member invite and reflates repo and account")



    flag.Parse()

    pn, err := NewPnode(basePath, *init)
    if err != nil {
        log.Fatal(err)
    }

    // Seed a new repo?
    if len(*seed) > 0 {
        buf, err := ioutil.ReadFile(*seed)
        if err != nil {
            log.Fatal(err)
        }

        seed := &repo.MemberSeed{}
        if err = seed.Unmarshal(buf); err != nil {
            log.Fatal(err)
        }

        if err = pn.SeedRepo(seed.RepoSeed); err != nil {
            log.WithError(err).Fatalf("seed failed from %v", *seed)
        }
    }

    {
        intr, intrCtx := plan.SetupInterruptHandler(context.Background())
        defer intr.Close()
        
        pnCtx, err := pn.Startup(intrCtx)
        if err != nil {
            log.WithError(err).Fatalf("failed to startup repo node")
        } else {

            log.Infof("to stop service: kill -s SIGINT %d\n", os.Getpid())

            select {
                case <- pnCtx.Done():
            }

            pn.Shutdown("task complete")
        }
    }

}



