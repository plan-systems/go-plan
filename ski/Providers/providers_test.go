package main

import (
	"bytes"
    "math/rand"
    "fmt"
    //"log"

    "testing"

	"github.com/plan-tools/go-plan/ski"
	"github.com/plan-tools/go-plan/plan"


	"github.com/plan-tools/go-plan/ski/Providers/nacl"

)




var gTesting *testing.T


func TestCommunityEncryption(t *testing.T) {

    gTesting = t

    providersToTest := []string {}

    ski.RegisterProvider(nacl.Provider)

    // Register providers to test 
    providersToTest = append(providersToTest, 
        nacl.Provider.InvocationStr(),
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

    // 1) make a new community key
	opResults, err := A.doOp(ski.OpArgs{
        OpName: ski.OpCreateCommunityKey,
    })
	if err != nil {
		gTesting.Fatal(err)
    }
    communityKeyID := plan.GetKeyID(opResults.Content)

    fmt.Printf("%s's encryptPubKey %v\n", A.name, A.encryptPubKey)
    fmt.Printf("%s's encryptPubKey %v\n", B.name, B.encryptPubKey)

    // 2) generate a xfer community key msg from A
    opResults, err = A.doOp(ski.OpArgs{
        OpName: ski.OpSendCommunityKeys,
        OpKeyIDs: []plan.KeyID{communityKeyID},
        PeerPubKey: B.encryptPubKey,
        CryptoKeyID: A.encryptPubKeyID,
    })
    if err != nil {
        gTesting.Fatal(err)
    }
    
    // 3) insert the new community key into B
    opResults, err = B.doOp(ski.OpArgs{
        OpName: ski.OpAcceptCommunityKeys,
        Msg: opResults.Content,
        PeerPubKey: A.encryptPubKey,
        CryptoKeyID: B.encryptPubKeyID,
    })
    if err != nil {
        gTesting.Fatal(err)
    }

	clearMsg := []byte("hello, PLAN community!")

    // Encrypt a new community msg on A
	opResults, err = A.doOp(ski.OpArgs{
        OpName: ski.OpEncryptForCommunity,
        CryptoKeyID: communityKeyID,
        Msg: clearMsg,
    })
	if err != nil {
		gTesting.Fatal(err)
	}

    encryptedMsg := opResults.Content

    // Send the encrypted community message to B
	opResults, err = B.doOp(ski.OpArgs{
        OpName: ski.OpDecryptFromCommunity,
        CryptoKeyID: communityKeyID,
        Msg: encryptedMsg,
    })
	if err != nil {
		gTesting.Fatal(err)
    }

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

        _, err = B.doOp(ski.OpArgs{
            OpName: ski.OpDecryptFromCommunity,
            CryptoKeyID: communityKeyID,
            Msg: badMsg,
        })
        if err == nil {
            gTesting.Fatal("there should have been a decryption error!")
        }
    }
    
}













type testSession struct {
    name        string
    session     ski.Session
    blocker     chan int   
    signingKeyID plan.KeyID

    encryptPubKeyID plan.KeyID 
    encryptPubKey   []byte
}




func (ts *testSession) doOp(inOpArgs ski.OpArgs) (*plan.Block, *plan.Perror) {

    var outErr *plan.Perror
    var outResults *plan.Block

    ts.session.DispatchOp(&inOpArgs, func(opResults *plan.Block, inErr *plan.Perror) {
        outErr = inErr
        outResults = opResults

        ts.blocker <- 1
    })

    <- ts.blocker

    return outResults, outErr
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



// test setup helper
func newSession(inInvocation plan.Block, inName string) *testSession {

    ts := &testSession{
        name:inName,
        blocker:make(chan int, 100),
    }

    var err *plan.Perror
    ts.session, err = ski.StartSession(
        inInvocation,
        ski.GatewayRWAccess,
        nil,
    )
    if err != nil {
        gTesting.Fatal(err)
    }

    identityResults, err := ts.doOp(
        ski.OpArgs{
            OpName: ski.OpNewIdentityRev,
        })
    if err != nil {
        gTesting.Fatal(err)
    }


    ts.signingKeyID = plan.GetKeyID( identityResults.GetContentWithLabel(ski.PubSigningKeyName) )

    ts.encryptPubKey = identityResults.GetContentWithLabel(ski.PubCryptoKeyName)
    ts.encryptPubKeyID = plan.GetKeyID( ts.encryptPubKey )

    return ts
}


