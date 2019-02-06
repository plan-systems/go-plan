
package main

import (
    "fmt"
    //"flag"
    log "github.com/sirupsen/logrus"
    "time"
    "io"
    "bytes"
    //"fmt"
	//"os/user"
    //"path"
    //"crypto/md5"
    "github.com/plan-systems/go-plan/ski/Providers/hive"
	//_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
    "encoding/hex"

    //"github.com/plan-systems/go-plan/pnode"

    //"github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/pservice"

    "context"
 	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"

    ds "github.com/plan-systems/go-plan/pdi/StorageProviders/datastore"

    //"github.com/plan-systems/go-plan/ski/Providers/filesys"

)

// Pnode represents a pnode daemon
type Pnode struct {

    ShuttingDown chan bool
    SP           pdi.StorageProviderClient
}


/*
Community genesis:

1) Community genesis 
    - Generate StorageProvider key/address (for genesis admin)
    - Init StorageProvider w/ genesis block (assigns bulk $ to admins)
    - Init community's reserved channels
2) Create new member
    - Generate SP key/address 
    - Assign $ to new SP ID
    - Add member to Member Epoch channel





*/


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
	conn, err := grpc.Dial("localhost:50057", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

    var session *pdi.StorageSession

	pn.SP = pdi.NewStorageProviderClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

    cid, _ := hex.DecodeString("78c16a9d533e670434e7c555acd21a59c0a26faa58c390ed")
    authorKey, _ := hex.DecodeString("4390a54640bd532dffa5decc45153b51bc8fe9ac94e69b42f33aaacf678fdb45")
		
    {
        sessReq := pservice.SessionRequest{
            CommunityId: cid,
        }

        var header, trailer metadata.MD
        session, err = pn.SP.StartSession(ctx, &sessReq, grpc.Header(&header), grpc.Trailer(&trailer))
        if err != nil {
            log.Fatalf("could not StartSession: %v", err)
        }
        
        ctx, err = pservice.TransferSessionToken(ctx, trailer)
        if err != nil {
            log.WithError(err).Errorf("TransferSessionToken failed")
        }
    }


    {
        
        {
            var (
                err error
                st *ski.SessionTool
            )
            st, err = ski.NewSessionTool(
                hive.NewProvider(),
                "genesis-admin",
                cid,
            )
            if err != nil {
                log.Fatal(err)
            }


            // Create the admin acct
            encoder, err := ds.NewTxnEncoder(0)
            err = encoder.ResetSession(
                session.TxnEncoderInvocation,
                st.Session,
                st.CommunityID,
            )
            if err != nil {
                log.Fatal(err)
            }

            encoder.ResetAuthorID(ski.PubKey{
                KeyDomain: ski.KeyDomain_PERSONAL,
                Bytes: authorKey,
            })

            newAcct, err := encoder.GenerateNewAccount()


            xferAddr := newAcct.Bytes
            log.Infof("Depositing to acct: 0x%s", hex.EncodeToString(xferAddr))

            //config.Epoch.GenesisID = pubKey.Base256()

            //ts := newSession(cid, "Alice")
            var queryStartTime int64
            var testPayloads [20][]byte
            for i := range testPayloads {
                payload := make([]byte, 1000)
                for j := 0; j < len(payload); j++ {
                    payload[j] = byte(i & 0xFF)
                }
                testPayloads[i] = payload
            }

            txnsToExpectBack := 3
            txnReplayStart :=  len(testPayloads) - 5 

            for i, payload := range testPayloads {

                txns, err := encoder.EncodeToTxns(
                    payload,
                    []byte(fmt.Sprintf("txn %d", i)),
                    pdi.PayloadCodec_Unspecified,
                    []*pdi.Transfer{
                        &pdi.Transfer{
                            To: xferAddr,
                            Gas: int64(1000 + i),
                        },
                    },
                )
                if err != nil {
                    log.Fatal(err)
                }

                if i == txnReplayStart {
                    queryStartTime = txns[0].TxnInfo.TimeSealed
                }

                for _, txn := range txns {
                    rawTxn := pdi.ReadiedTxn{
                        RawTxn: txn.RawTxn,
                    }

                    utid := pdi.FormUTID("", txn.TxnInfo.TimeSealed, txn.TxnInfo.TxnHashname)
                    timeCode := pdi.FormUTID("", txn.TxnInfo.TimeSealed, nil)
                    log.Infof("Committing txn %s, TimeSealed %d, timeCode: %s", utid, txn.TxnInfo.TimeSealed, timeCode)
                    conn, err := pn.SP.CommitTxn(ctx, &rawTxn)
                    if err != nil {
                        log.WithError(err).Warn("CommitTxn() returned FATAL error")
                        break
                    }

                    for {
                        txnMetaInfo, err := conn.Recv() 

                        if err == io.EOF {
                            break
                        }

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

                    waitSecs(1)
                    //log.Printf("got rmsg %v, %v", msg.Label, msg.SigBlock)
                }
            }

            {
                decoder := ds.NewTxnDecoder()

                log.Infof("Seeking to time  %d", queryStartTime)
                conn, connErr := pn.SP.Query(ctx, &pdi.TxnQuery{
                    TimestampMin: queryStartTime,
                    TimestampMax: queryStartTime + int64(txnsToExpectBack),
                })

                for i := 0; connErr == nil; i++ {
                    txnBundle, connErr := conn.Recv()
                    if connErr != nil {      // io.EOF
                        break
                    }

                    txnCount := 0 
                    for _, txn := range txnBundle.Txns {
                        txnCount++
                        
                        var txnInfo pdi.TxnInfo
                        var txnSeg pdi.TxnSegment

                        log.Infof("Recieved txn UTID %v", txn.UTID)
                        err = decoder.DecodeRawTxn(
                            txn.RawTxn,
                            &txnInfo,
                            &txnSeg,
                        )
                        if err != nil {
                            log.Fatal(err)
                        }
                        n := txnReplayStart + txnCount -1
                        if bytes.Compare(txnSeg.SegData, testPayloads[n]) == 0 {
                            log.Infof("%d of %d checked!", txnCount, txnsToExpectBack)
                        } else {
                            log.Fatalf("failed check #%d", i)
                        }
                    }
                }

            }

            st.EndSession("donezo")

        }
    }

    
    //waitSecs(140)
    

}


func waitSecs(secs int){

    log.Printf("Waiting %vs.", secs)
    time.Sleep( time.Second * time.Duration(secs) );
}






/*  /*
            select {

                case <-pn.ShuttingDown: 
                    log.Print("ShuttingDown!") 
                    done = true
            }
            */

 /*
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
                }

        */


/*

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



func (ts *testSession) doOpWithErr(inOpArgs ski.OpArgs) (*plan.Block, *plan.Err) {

    var outErr *plan.Err
    var outResults *plan.Block

    ts.session.DispatchOp(inOpArgs, func(opResults *plan.Block, inErr *plan.Err) {
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

    var err error
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
        func(inKeys []*ski.KeyEntry, inErr error) {
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

    ts.session.EndSession(inReason, func(inParam interface{}, inErr error) {
        if inErr != nil {
            log.Fatal(inErr)
        }
        ts.blocker <- 1
    })

    <- ts.blocker

}




*/