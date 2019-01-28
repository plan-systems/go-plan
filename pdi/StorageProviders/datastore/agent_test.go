package datastore

import (
	"bytes"
    "testing"
    "math/rand"
    //"io/ioutil"
    //"os"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"

    "fmt"
	"os/user"
    "path"
    //"time"
    "os"
    "crypto/md5"
    //"ioutil"


    "github.com/plan-systems/go-plan/ski/Providers/filesys"

	_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

)


var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

var gTesting *testing.T
var gCommunityID = [4]byte{0, 1, 2, 3}
var gDefaultFileMode = os.FileMode(0775)




func TestVarAppendBuf(t *testing.T) {

    gTesting = t

	seed := plan.Now().UnixSecs
	gTesting.Logf("Using seed: %d", seed)
	rand.Seed(seed)

 
    testBufs := make([][]byte, 100)
    
    for i := range(testBufs) {
    	N := int(1 + rand.Int31n(int32(len(gTestBuf))-2))
        testBufs[i] = []byte(gTestBuf[:N])
    }

    buf := make([]byte, len(testBufs) * len(gTestBuf))
    totalLen := 0

    var err error
    for i := range(testBufs) {
        totalLen, err = pdi.AppendVarBuf(buf, totalLen, testBufs[i])
        if err != nil {
            t.Fatal(err)
        }
    }
    buf = buf[:totalLen]

    offset := 0
    var payload []byte

    for i := range(testBufs) {
        offset, payload, err = pdi.ReadVarBuf(buf, offset)
        if err != nil {
            t.Fatal(err)
        }
        if bytes.Compare(payload, testBufs[i]) != 0 {
            t.Fatalf("'%v' != '%v'", string(payload), string(testBufs[i]))
        }
    }

    if offset != len(buf) {
        t.Fatalf("expected offset == %v, got %v'", len(buf), offset)
    } 
}



func getTmpDir(inSubDir string) string {

	usr, err := user.Current()
	if err != nil {
		gTesting.Fatal(err)
	}

	tmpDir := path.Join(usr.HomeDir, "_plan-testing")
    if len(inSubDir) > 0 {
        tmpDir = path.Join(tmpDir, inSubDir)
    }

    err = os.MkdirAll(tmpDir, gDefaultFileMode)
	if err != nil {
		gTesting.Fatal(err)
	}

    return tmpDir
}


func TestFileSysSKI(t *testing.T) {

    gTesting = t

    // Register providers to test 
    providersToTest := []string{}
    providersToTest = append(providersToTest, 
        filesys.Provider.InvocationStr(),
    )

    for _, invocationStr := range providersToTest {

        invocation := plan.Block{
            Label: invocationStr,
        }

        A := newSession(invocation, "Alice")
        B := newSession(invocation, "Bob")

        doCoreTests(A, B)

        A.endSession("done A")
        B.endSession("done B")
    }

}





func doCoreTests(A, B *testSession) {

    fmt.Printf("%s's encryptPubKey %v\n", A.name, A.encryptPubKey)
    fmt.Printf("%s's encryptPubKey %v\n", B.name, B.encryptPubKey)

    // 1) Generate a new community key (on A)
    communityKey := A.generateNewKey(ski.KeyType_SYMMETRIC_KEY, ski.KeyDomain_COMMUNITY)
 
    // 2) export the community key from A
    opResults := A.doOp(ski.OpArgs{
        OpName: ski.OpExportNamedKeys,
        OpKeySpec: *A.encryptPubKey,
        CommunityID: gCommunityID[:],
        KeySpecs: []*ski.PubKey{
            communityKey,
        },
        PeerPubKey: B.encryptPubKey.Base256(),
    })

    // 3) insert the new community key into B
    opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpImportKeys,
        Msg: opResults.Content,
        OpKeySpec: *B.encryptPubKey,
        CommunityID: gCommunityID[:],
        PeerPubKey: A.encryptPubKey.Base256(),
    })


	clearMsg := []byte("hello, PLAN community!")

    // 4) Encrypt a new community msg (sent from A)
	opResults = A.doOp(ski.OpArgs{
        OpName: ski.OpEncrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg: clearMsg,
    })

    encryptedAtoB := opResults.Content

    // 5) Send the encrypted community message to B
	opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpDecrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg:  encryptedAtoB,
    })

    decryptedMsg := opResults.Content

    // 6) Now check that B can send an encrypted community msg to A
	opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpEncrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg: decryptedMsg,
    })
    encryptedBtoA := opResults.Content
	opResults = A.doOp(ski.OpArgs{
        OpName: ski.OpDecrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg: encryptedBtoA,
    })

    // 7) Did the round trip work?
    //    clearMsg => A => encryptedAtoB => B => decryptedMsg => B => encryptedBtoA => A => opResults.Content
	if ! bytes.Equal(clearMsg, opResults.Content) {
		gTesting.Fatalf("expected %v, got %v after decryption", clearMsg, opResults.Content)
    }

    badMsg := make([]byte, len(encryptedAtoB))

    // Vary the data slightly to test 
    for i := 0; i < 1000; i++ {

        rndPos := rand.Int31n(int32(len(encryptedAtoB)))
        rndAdj := 1 + byte(rand.Int31n(254))
        copy(badMsg, encryptedAtoB)
        badMsg[rndPos] += rndAdj

        _, opErr := B.doOpWithErr(ski.OpArgs{
            OpName: ski.OpDecrypt,
            OpKeySpec: *communityKey,
            CommunityID: gCommunityID[:],
            Msg: badMsg,
        })
        if opErr == nil {
            gTesting.Fatal("there should have been a decryption error!")
        }
    }

    // Test agent encode/decode
    {
        blobBuf := make([]byte, 500000)
        agent, _ := NewAgent("", 1000)

        for i := 0; i < 100; i++ {
            
            blobLen := int(rand.Int31n(1+rand.Int31n(5000)))

            payload := blobBuf[:blobLen]
            rand.Read(payload)

            txns, err := agent.EncodeToTxns(
                payload,
                []byte{4, 3, 2, 1},
                pdi.PayloadCodec_Unspecified,
                A.session,
                A.signingPubKey,
                gCommunityID[:],
            )
            if err != nil {
                gTesting.Fatal(err)
            }

            for _, txn := range txns {
                decodedInfo := pdi.TxnInfo{}
                decodedSeg := pdi.TxnSegment{}

                err := agent.DecodeRawTxn(
                    txn.RawTxn,
                    &decodedInfo,
                    &decodedSeg,
                )
                if err != nil {
                    gTesting.Fatal(err)
                }

                b1, _ := decodedInfo.Marshal()
                b2, _ :=  txn.TxnInfo.Marshal()
                if bytes.Compare(b1, b2) != 0 {
                    gTesting.Fatal("txn seg info check failed")
                }
                //if Bytes.Compare(decodedSeg.SegData, txn.
            }
        }
    }
}





type testSession struct {
    name        string
    session     ski.Session
    blocker     chan int   
    signingPubKey *ski.PubKey
    encryptPubKey *ski.PubKey
}




func (ts *testSession) doOp(inOpArgs ski.OpArgs) *plan.Block {


    results, err := ts.doOpWithErr(inOpArgs)

    if err != nil {
        gTesting.Fatal(err)
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






// test setup helper
func newSession(inInvocation plan.Block, inName string) *testSession {

    ts := &testSession{
        name:inName,
        blocker:make(chan int, 1),
    }

    userID := md5.Sum([]byte(inName))

    var err *plan.Perror
    ts.session, err = ski.StartSession(ski.SessionParams{
        Invocation: inInvocation,
        UserID: userID[:],
        BaseDir: getTmpDir(inName),
    })

    if err != nil {
        gTesting.Fatal(err)
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
        gCommunityID[:], 
        []*ski.PubKey{
            &ski.PubKey{
                KeyType: inKeyType,
                KeyDomain: inKeyDomain,
            },
        },
        func(inKeys []*ski.KeyEntry, inErr *plan.Perror) {
            if inErr != nil {
                gTesting.Fatal(inErr)
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
            gTesting.Fatal(inErr)
        }
        ts.blocker <- 1
    })

    <- ts.blocker

}









