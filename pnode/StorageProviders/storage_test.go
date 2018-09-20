package main


import (
    "testing"
    "os"
    "os/user"
    "path"
    "math"
    "math/rand"
    //"fmt"
	"encoding/hex"
    "time"
    "bytes"
    "log"

    "github.com/plan-tools/go-plan/plan"
    "github.com/plan-tools/go-plan/pdi"
    "github.com/plan-tools/go-plan/pnode/StorageProviders/bolt"


)


var gTesting *testing.T




type testPB struct {
    reqIDs          []pdi.RequestID
    numCommitted    int
    numToCommit     int
    storageMsgCh    <-chan pdi.StorageMsg
    session         pdi.StorageSession
}




func Test1(t *testing.T) {

    gTesting = t

    seed := plan.Now().UnixSecs
    log.Printf("Using seed: %d", seed)
    rand.Seed( seed )


    provider := bolt.NewBoltProvider( 
        getTmpDir(),
        os.FileMode(0775),
    )

    var err error
    var communityID plan.CommunityID
    for i := 0; i < len(communityID); i++ {
        communityID[i] = 1
    }

    N := 100 //int(40 + rand.Int31n(50))

    pb := &testPB{
        numToCommit: N,
        reqIDs: make([]pdi.RequestID, N),
        numCommitted: 0,
    }


    pb.session, err = provider.StartSession(
        communityID[:],
    )
    if err != nil {
        log.Fatal( err )
    }

    pb.storageMsgCh = pb.session.GetMsgChan()

    /*****************************************************
    ** Wait for the session to be ready
    **/

    for {
        msg, ok := <-pb.storageMsgCh
        if  ! ok {
            log.Fatal("channel unexpectedly closed")
        }
        if msg.AlertCode == pdi.SessionIsReady {
            if ! pb.session.IsReady() {
                log.Fatal("session should be ready")
            }
            break
        } else {
            if pb.session.IsReady() {
                log.Fatal("session should not be ready")
            }   
        }
    }


    waitForTest := make(chan bool)

    pb.session.RequestFromBookmark(nil)


    go func() {

        /*****************************************************
        ** Process and verify committed txns
        **/

        sessionEnded := false

        for ! sessionEnded {
            
            msg, ok := <-pb.storageMsgCh
            if ok {
                switch {

                    case msg.AlertCode == 0:
                        switch msg.StorageOp {
                            
                            case pdi.OpTxnReport, pdi.OpRequestTxns:
                                for _, txn := range msg.Txns {
                                    verifyRandoBody(*txn.Body)
                                }
                                
                                // If we verified the last txn, close the session, ending the test
                                if pb.numToCommit == pb.numCommitted && msg.StorageOp == pdi.OpRequestTxns {
                                    pb.session.EndSession("Basic test complete!")
                                }

                            case pdi.OpCommitTxns: {
                                go func() {
                                    for _, txn := range msg.Txns {
                                        pb.session.RequestTxns( []pdi.TxnRequest{
                                            pdi.TxnRequest{
                                                TxnName: txn.TxnName,
                                                TimeCommitted: txn.TimeCommitted,
                                                IncludeBody: true,
                                            },
                                        })
                                    }
                                }()
                            
                            }
                        }

                    case ( msg.AlertCode & pdi.SessionEndedAlertMask ) != 0:
                        log.Printf("Session ended (%v): %v", msg.AlertCode, msg.AlertMsg)
                        sessionEnded = true
                        break

                    default:
                        gTesting.Fatalf("Got alert code: %v", msg.AlertCode)
                        
                }
            }

        }

        waitForTest <-true
    }()


    go func() {

        /*****************************************************
        ** Commit dummy txns
        **/
        
        for i := 0; i < pb.numToCommit; i++ {

            var err error
            body := makeRandoBody(i+1)
            pb.reqIDs[i], err = pb.session.CommitTxn(body)
            if err != nil {
                log.Fatal(err)
            }
            log.Printf("Committed %d (of %d)\n", pb.numCommitted + 1, pb.numToCommit)

            pb.numCommitted++
            {
                sleepMSf := rand.NormFloat64() * 200 + 50
                sleepMS:= math.Min( math.Max(0, sleepMSf), 800 )
                time.Sleep(time.Millisecond * time.Duration(sleepMS))
            }
        }

        log.Printf("%d commits COMPLETE\n", pb.numCommitted)

    }()

    ok := <- waitForTest
    if ! ok {
        gTesting.Fatal("test FAILED")
    }
}




func getTmpDir() string {

    usr, err := user.Current()
    if err != nil {
        gTesting.Fatal(err)
    }

    return path.Join( usr.HomeDir, "plan-testing" )

}



func makeRandoBody(idx int) *plan.Block {

    var blobBuf [20000]byte


    blobLen := int(rand.Int31n( 1 + rand.Int31n(100) ) * rand.Int31n(100) + 1)

    blob := blobBuf[:blobLen]
    rand.Read(blob)

    numCopies := ( 1 + int(blob[0]) + idx%5 )
    contentSz := numCopies * blobLen

    body := &plan.Block{
        Label: hex.EncodeToString(blob),
        Codec: "/plan/test/" + string(idx),
        CodecCode: uint32(idx),
        Content: make([]byte, contentSz),
    }

    log.Printf("Making txn (#%d) body blobLen: %d, contentSz: %d\n", idx, blobLen, contentSz)

    pos := 0
    for j := 0; j < numCopies; j++ {
        copy(body.Content[pos:pos+blobLen], blob)
        pos += blobLen
    }

    return body
}


func verifyRandoBody(body plan.Block) {
    blob, err := hex.DecodeString(body.Label)
    if err != nil {
        gTesting.Fatal(err)
    }

    idx := int(body.CodecCode)
    blobLen := len(blob)
    numCopies := ( 1 + int(blob[0]) + idx%5 )

    if body.Codec != "/plan/test/" + string(idx) {
        gTesting.Fatalf("Failed Codec data test")
    }

    if len(body.Content) != numCopies * blobLen {
        gTesting.Fatalf("Got length %d, expected %d", len(body.Content), numCopies * blobLen)
    }

    pos := 0
    for j := 0; j < numCopies; j++ {
        if body.Content[pos] != blob[0] {
            gTesting.Fatalf("First byte of data chk failed",)
        }
        if ! bytes.Equal(body.Content[pos:pos+blobLen], blob) {
            gTesting.Fatalf("On content copy %d, data chk failed", j)
        }
        pos += blobLen
    }

    log.Printf("Verified txn (#%d)\n", idx)

}

