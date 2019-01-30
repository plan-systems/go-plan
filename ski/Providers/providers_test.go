package main

import (
	"bytes"
    "math/rand"
    "fmt"
    //"time"
    //"ioutil"

    "testing"

	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"

    "github.com/plan-systems/go-plan/ski/Providers/filesys"

	_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

)



var gTesting *testing.T
var gCommunityID = [4]byte{0, 1, 2, 3}



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
 
    // 2) generate a xfer community key msg from A
    opResults := A.doOp(ski.OpArgs{
        OpName: ski.OpExportNamedKeys,
        OpKeySpec: ski.PubKey{
            KeyDomain: ski.KeyDomain_PERSONAL,
            KeyBase: A.encryptPubKey,
        },
        CommunityID: gCommunityID[:],
        KeySpecs: []*ski.PubKey{
            communityKey,
        },
        PeerPubKey: B.encryptPubKey,
    })

    // 3) insert the new community key into B
    opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpImportKeys,
        Msg: opResults.Content,
        OpKeySpec: ski.PubKey{
            KeyDomain: ski.KeyDomain_PERSONAL,
            KeyBase: B.encryptPubKey,
        },
        CommunityID: gCommunityID[:],
        PeerPubKey: A.encryptPubKey,
    })


	clearMsg := []byte("hello, PLAN community!")

    // 4) Encrypt a new community msg (sent from A)
	opResults = A.doOp(ski.OpArgs{
        OpName: ski.OpEncrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg: clearMsg,
    })

    encryptedMsg := opResults.Content

    // 5) Send the encrypted community message to B
	opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpDecrypt,
        OpKeySpec: *communityKey,
        CommunityID: gCommunityID[:],
        Msg: encryptedMsg,
    })

	if ! bytes.Equal(clearMsg, opResults.Content) {
		gTesting.Fatalf("expected %v, got %v after decryption", clearMsg, opResults.Content)
    }

    badMsg := make([]byte, len(encryptedMsg))

    // Vary the data slightly to test 
    for i := 0; i < 1000; i++ {

        rndPos := rand.Int31n(int32(len(encryptedMsg)))
        rndAdj := 1 + byte(rand.Int31n(254))
        copy(badMsg, encryptedMsg)
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
}





type testSession struct {
    ski.SessionTool 

    encryptPubKey []byte
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
    }


    ts.encryptPubKey = ts.GenerateNewKey(ski.KeyType_ASYMMETRIC_KEY, ski.KeyDomain_PERSONAL).Base256()

    return ts
}




