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

    "github.com/plan-systems/go-plan/ski/Providers/hive"

)



var gTesting *testing.T
var gCommunityID = [4]byte{0, 1, 2, 3}


func TestFileSysSKI(t *testing.T) {

    gTesting = t

    // Register providers to test 
    providersToTest := []func() ski.Provider{
        func() ski.Provider {
            return hive.NewProvider()
        },
    }

    for _, providerFactory := range providersToTest {

        A := newSession(providerFactory(), "Test-Alice")
        B := newSession(providerFactory(), "Test-Bob")

        fmt.Printf("%s's encryptPubKey %v\n", A.UserName, A.encryptPubKey)
        fmt.Printf("%s's encryptPubKey %v\n", B.UserName, B.encryptPubKey)

        doProviderTest(A, B)

        A.EndSession("done A")
        B.EndSession("done B")
    }

}




func doProviderTest(A, B *testSession) {

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
        PeerPubKey: B.encryptPubKey.Bytes,
    })

    // 3) insert the new community key into B
    opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpImportKeys,
        Msg: opResults.Content,
        OpKeySpec: *B.encryptPubKey,
        CommunityID: gCommunityID[:],
        PeerPubKey: A.encryptPubKey.Bytes,
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
}


type testSession struct {
    ski.SessionTool 

    encryptPubKey *ski.PubKey
}




func (ts *testSession) doOp(inOpArgs ski.OpArgs) *plan.Block {

    results, err := ts.DoOp(inOpArgs)

    if err != nil {
        gTesting.Fatal(err)
    }
    return results
}







// test setup helper
func newSession(skiProvider ski.Provider, inUserName string) *testSession {

    tool, err := ski.NewSessionTool(
        skiProvider,
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


    ts.encryptPubKey = ts.GenerateNewKey(ski.KeyType_ASYMMETRIC_KEY, ski.KeyDomain_PERSONAL)

    return ts
}




