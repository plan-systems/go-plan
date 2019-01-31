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
    //"time"
    "os"
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





func TestFileSysSKI(t *testing.T) {

    gTesting = t

    // Register providers to test 
    providersToTest := []string{}
    providersToTest = append(providersToTest, 
        filesys.Provider.InvocationStr(),
    )

    for _, invocationStr := range providersToTest {


        A := newSession(invocationStr, "Test-Alice")
        B := newSession(invocationStr, "Test-Bob")

        doCoreTests(A, B)

        A.EndSession("done A")
        B.EndSession("done B")
    }

}





func doCoreTests(A, B *testSession) {

    fmt.Printf("%s's encryptPubKey %v\n", A.UserName, A.encryptPubKey)
    fmt.Printf("%s's encryptPubKey %v\n", B.UserName, B.encryptPubKey)

    // 1) Generate a new community key (on A)
    communityKey := A.GenerateNewKey(ski.KeyType_SYMMETRIC_KEY, ski.KeyDomain_COMMUNITY)
 
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

        _, opErr := B.DoOp(ski.OpArgs{
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
        decoder := NewTxnDecoder()
        encoder, _ := NewTxnEncoder(1000)

        {
            err := encoder.ResetSession(
                decoder.TxnEncoderInvocation(),
                A.Session,
                gCommunityID[:],
            )
            if err != nil {
                gTesting.Fatal(err)
            }

            err = encoder.ResetAuthorID(*A.signingPubKey)
            if err != nil {
                gTesting.Fatal(err)
            }
        }

        for i := 0; i < 100; i++ {
            
            blobLen := int(rand.Int31n(1+rand.Int31n(5000)))

            payload := blobBuf[:blobLen]
            rand.Read(payload)

            txns, err := encoder.EncodeToTxns(
                payload,
                []byte{4, 3, 2, 1},
                pdi.PayloadCodec_Unspecified,
                nil,
            )
            if err != nil {
                gTesting.Fatal(err)
            }

            for _, txn := range txns {
                decodedInfo := pdi.TxnInfo{}
                decodedSeg := pdi.TxnSegment{}

                err := decoder.DecodeRawTxn(
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
    ski.SessionTool 

    encryptPubKey *ski.PubKey
    signingPubKey *ski.PubKey
}




func (ts *testSession) doOp(inOpArgs ski.OpArgs) *plan.Block {

    results, err := ts.DoOp(inOpArgs)

    if err != nil {
        gTesting.Fatal(err)
    }
    return results
}







// test setup helper
func newSession(inInvocationStr string, inUserName string) *testSession {


    tool, err := ski.NewSessionTool(
        inInvocationStr,
        inUserName,
        gCommunityID[:],
    )
    if err != nil {
        gTesting.Fatal(err)
    }

    ts := &testSession{
        *tool,
        nil,
        nil,
    }


    ts.encryptPubKey = ts.GenerateNewKey(ski.KeyType_ASYMMETRIC_KEY, ski.KeyDomain_PERSONAL)
    ts.signingPubKey = ts.GenerateNewKey(ski.KeyType_SIGNING_KEY, ski.KeyDomain_PERSONAL)

    return ts
}






