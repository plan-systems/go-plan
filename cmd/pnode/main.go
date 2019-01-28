
package main

import (
    "os"
    //"flag"
    log "github.com/sirupsen/logrus"
    "time"
    //"io"
    //"fmt"
	"os/user"
    "path"
    "crypto/md5"
    "github.com/plan-systems/go-plan/ski/Providers/filesys"
	_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
    "encoding/hex"

    //"github.com/plan-systems/go-plan/pnode"

    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/pservice"

    "context"
 	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

)


type Pnode struct {

    ShuttingDown chan bool
    SP           pdi.StorageProviderClient
}




func main() {

/*
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

    pn.Run()*/

    pn := Pnode{
        ShuttingDown: make(chan bool),
    }

	// Set up a connection to the server.
	conn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

    var session *pdi.StorageSession

	pn.SP = pdi.NewStorageProviderClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 120)
	defer cancel()

    cid, _ := hex.DecodeString("7b7c4a7ac0d11289ebd545b324b5c764f900786820a2f622")

    {
        sessReq := pservice.SessionRequest{
            CommunityId: cid,
        }

        var header, trailer metadata.MD
        session, err = pn.SP.StartSession(ctx, &sessReq, grpc.Header(&header), grpc.Trailer(&trailer))
        if err != nil {
            log.Fatalf("could not StartSession: %v", err)
        }

        var perr *plan.Perror
        
        ctx, perr = pservice.TransferSessionToken(ctx, trailer)
        if perr != nil {
            log.WithError(perr).Errorf("TransferSessionToken failed")
        }
    }


    go func() {
        done := false
        for ! done {

            /*
            select {

                case <-pn.ShuttingDown: 
                    log.Print("ShuttingDown!") 
                    done = true
            }
            */

            /*
            msg, err := msgInlet.Recv()
            if err != nil {
                if io.EOF == err {
                    log.Print("Recv() received io.EOF") 
                } else {
                    log.Printf("Recv() received err %v", err) 
                }
                break
            } */
            agent, err := ds.NewAgent(session.RequiredAgent, 0)
            if err != nil {
                log.Fatalf("could not create agent: %v", err)
            }

            ts := newSession(cid, "Alice")

            for i := 0; i < 1; i++ {
                payload := make([]byte, 1000)
                for j := 0; j < len(payload); j++ {
                    payload[j] = byte(i & 0xFF)
                }

                txns, err := agent.EncodeToTxns(
                    payload,
                    []byte("yoyoyo!"),
                    pdi.PayloadCodec_Unspecified,
                    ts.session,
                    ts.signingPubKey,
                    cid,
                )
                if err != nil {
                    log.Fatal(err)
                }

                for _, txn := range txns {
                    rawTxn := pdi.RawTxn{
                        TxnData: txn.RawTxn,
                    }

                    rmsg, err := pn.SP.CommitTxn(ctx, &rawTxn)
                    if err != nil {
                        log.WithError(err).Warn("CommitTxn() returned FATAL error")
                        break
                    }

                    for {
                        txnMetaInfo, err := rmsg.Recv() 

                        if txnMetaInfo != nil {
                            log.WithFields(log.Fields{
                                "TxnStatus": pdi.TxnStatus_name[int32(txnMetaInfo.TxnStatus)],
                            }).Info("CommitTxn()")
                        }
                        
                        if err != nil {
                            log.WithError(err).Warn("CommitTxn() returned error")
                            break
                        }

                        
                    }


                    //log.Printf("got rmsg %v, %v", msg.Label, msg.SigBlock)
                }
/*
                if perr != nil {
                    log.WithError(perr).Warn("PostReponse() complete")
                } else {
                    log.WithFields(log.Fields{
                        "status": rmsg.Status,
                    }).Info("PostReponse() complete")
                }*/

            }

            ts.endSession("done!")

            done = true
        }
    }()


    waitSecs(140)
    

}


func waitSecs(secs int){

    log.Printf("Waiting %vs.", secs)
    time.Sleep( time.Second * time.Duration(secs) );
}








type testSession struct {
    name        string
    session     ski.Session
    blocker     chan int   
    CommunityID []byte
    signingPubKey *ski.PubKey
    encryptPubKey *ski.PubKey
}

var gDefaulFileMode = os.FileMode(0775)



func (ts *testSession) doOp(inOpArgs ski.OpArgs) *plan.Block {

    results, err := ts.doOpWithErr(inOpArgs)

    if err != nil {
        log.Fatal(err)
    }
    return results
}



func (ts *testSession) doOpWithErr(inOpArgs ski.OpArgs) (*plan.Block, *plan.Perror) {

    var outErr *plan.Perror
    var outResults *plan.Block

    ts.session.DispatchOp(inOpArgs, func(opResults *plan.Block, inErr *plan.Perror) {
        outErr = inErr
        outResults = opResults

        ts.blocker <- 1
    })

    <- ts.blocker


    return outResults, outErr
}




func getTmpDir(inSubDir string) string {

	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	tmpDir := path.Join(usr.HomeDir, "_plan-testing")
    if len(inSubDir) > 0 {
        tmpDir = path.Join(tmpDir, inSubDir)
    }

    err = os.MkdirAll(tmpDir, plan.DefaultFileMode)
	if err != nil {
		log.Fatal(err)
	}

    return tmpDir
}



// test setup helper
func newSession(inCID []byte, inName string) *testSession {

    ts := &testSession{
        name:inName,
        blocker:make(chan int, 1),
        CommunityID: inCID,
    }

    userID := md5.Sum([]byte(inName))

    var err *plan.Perror
    ts.session, err = ski.StartSession(ski.SessionParams{
        Invocation: plan.Block{
            Label: filesys.Provider.InvocationStr(),
        },
        UserID: userID[:],
        BaseDir: getTmpDir(inName),
    })

    if err != nil {
        log.Fatal(err)
    }

    ts.encryptPubKey = ts.generateNewKey(ski.KeyType_ASYMMETRIC_KEY, ski.KeyDomain_PERSONAL)
    ts.signingPubKey = ts.generateNewKey(ski.KeyType_SIGNING_KEY,    ski.KeyDomain_PERSONAL)

    return ts
}

func (ts *testSession) generateNewKey(
    inKeyType ski.KeyType,
    inKeyDomain ski.KeyDomain,
) *ski.PubKey {

    var newKey *ski.PubKey

    ski.GenerateKeys(
        ts.session, 
        ts.CommunityID, 
        []*ski.PubKey{
            &ski.PubKey{
                KeyType: inKeyType,
                KeyDomain: inKeyDomain,
            },
        },
        func(inKeys []*ski.KeyEntry, inErr *plan.Perror) {
            if inErr != nil {
                log.Fatal(inErr)
            } else {
                newKey = &ski.PubKey{
                    KeyDomain: inKeys[0].KeyDomain,
                    KeyBase: inKeys[0].PubKey,
                }
            }

            ts.blocker <- 1
        },
    )

    <- ts.blocker

    return newKey

}



func (ts *testSession) endSession(inReason string) {

    ts.session.EndSession(inReason, func(inParam interface{}, inErr *plan.Perror) {
        if inErr != nil {
            log.Fatal(inErr)
        }
        ts.blocker <- 1
    })

    <- ts.blocker

}




