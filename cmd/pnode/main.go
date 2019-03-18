
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
    "github.com/plan-systems/go-plan/pdi"
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
    inflate     := flag.String( "inflate",      "",         "Reads the given member invite and reflates repo and account")



    flag.Parse()

    pn, err := NewPnode(basePath, *init)
    if err != nil {
        log.Fatal(err)
    }


    if len(*inflate) > 0 {
        buf, err := ioutil.ReadFile(*inflate)
        if err != nil {
            log.Fatal(err)
        }

        seed := &pdi.MemberSeed{}
        if err = seed.Unmarshal(buf); err != nil {
            log.Fatal(err)
        }

        if err = pn.InflateRepo(seed.RepoSeed); err != nil {
            log.WithError(err).Fatalf("inflate failed from %v", *inflate)
        }
    }


    {
        intr, intrCtx := plan.SetupInterruptHandler(context.Background())
        defer intr.Close()
        
        rnCtx, err := pn.Startup(intrCtx)
        if err != nil {
            log.WithError(err).Fatalf("failed to startup repo node")
        } else {

            log.Infof("to stop service: kill -s SIGINT %d\n", os.Getpid())

            go func() {
                buf, _ := ioutil.ReadFile("/Users/aomeara/go/src/github.com/plan-systems/go-plan/cmd/pdi-datastore/drewwww.plan.seed")

                seed := &pdi.MemberSeed{}
                seed.Unmarshal(buf)

                var workstationID [plan.CommunityIDSz]byte
                for i := 0; i < plan.CommunityIDSz; i++ {
                    workstationID[i] = byte(i)
                }

                ms, _, _ := pn.StartMemberSession(context.Background(), &repo.SessionReq{
                    WorkstationID: workstationID[:],
                    CommunityID: seed.RepoSeed.CommunityEpoch.CommunityID,
                    MemberEpoch: seed.MemberEpoch,
                })

                ms.GetItWorking()
            }()

            select {
                case <- rnCtx.Done():
            }

            pn.Shutdown("task complete")
        }
    }

}



