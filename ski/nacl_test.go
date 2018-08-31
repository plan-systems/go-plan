package ski // import "github.com/plan-tools/go-plan/ski"

import (
	"bytes"
    "math/rand"
    "fmt"

    "testing"

	"github.com/plan-tools/go-plan/pdi"
	"github.com/plan-tools/go-plan/plan"
)






func TestCommunityEncryption(t *testing.T) {

    A := newSession(t, "Alice")
    B := newSession(t, "Bob")

    // 1) make a new community key
	opResults, err := A.doOp(OpArgs{
        OpName: OpCreateCommunityKey,
    })
	if err != nil {
		t.Fatal(err)
    }
    var communityKeyID plan.KeyID
    communityKeyID.AssignFrom(opResults.Content)

    fmt.Printf("%s's encryptPubKey %v\n", A.name, A.encryptPubKey)
    fmt.Printf("%s's encryptPubKey %v\n", B.name, B.encryptPubKey)

    // 2) generate a xfer community key msg from A
    opResults, err = A.doOp(OpArgs{
        OpName: OpSendCommunityKeys,
        OpKeyIDs: []plan.KeyID{communityKeyID},
        PeerPubKey: B.encryptPubKey,
        CryptoKeyID: A.encryptPubKeyID,
    })
    if err != nil {
        t.Fatal(err)
    }
    
    // 3) insert the new community key into B
    opResults, err = B.doOp(OpArgs{
        OpName: OpAcceptCommunityKeys,
        Msg: opResults.Content,
        PeerPubKey: A.encryptPubKey,
        CryptoKeyID: B.encryptPubKeyID,
    })
    if err != nil {
        t.Fatal(err)
    }

	clearMsg := []byte("hello, PLAN community!")

    // Encrypt a new community msg on A
	opResults, err = A.doOp(OpArgs{
        OpName: OpEncryptForCommunity,
        CryptoKeyID: communityKeyID,
        Msg: clearMsg,
    })
	if err != nil {
		t.Fatal(err)
	}

    encryptedMsg := opResults.Content

    // Send the encrypted community message to B
	opResults, err = B.doOp(OpArgs{
        OpName: OpDecryptFromCommunity,
        CryptoKeyID: communityKeyID,
        Msg: encryptedMsg,
    })
	if err != nil {
		t.Fatal(err)
    }

	if ! bytes.Equal(clearMsg, opResults.Content) {
		t.Fatalf("expected %v, got %v after decryption", clearMsg, opResults.Content)
    }
    

    badMsg := make([]byte, len(encryptedMsg))

    // Vary the data slightly to test 
    for i := 0; i < 1000; i++ {

        rndPos := rand.Int31n(int32(len(encryptedMsg)))
        rndAdj := 1 + byte(rand.Int31n(254))
        copy(badMsg, encryptedMsg)
        badMsg[rndPos] += rndAdj

        _, err = B.doOp(OpArgs{
            OpName: OpDecryptFromCommunity,
            CryptoKeyID: communityKeyID,
            Msg: badMsg,
        })
        if err == nil {
            t.Fatal("there should have been a decryption error!")
        }
    }
    
}













type testSession struct {
    name        string
    session     Session
    blocker     chan int   
    signingKeyID plan.KeyID

    encryptPubKeyID plan.KeyID 
    encryptPubKey   []byte
}




func (ts *testSession) doOp(inOpArgs OpArgs) (*pdi.Block, *plan.Perror) {

    var outErr *plan.Perror
    var outResults *pdi.Block

    ts.session.DispatchOp(&inOpArgs, func(opResults *pdi.Block, inErr *plan.Perror) {
        outErr = inErr
        outResults = opResults

        ts.blocker <- 1
    })

    <- ts.blocker

    return outResults, outErr
}





// test setup helper
func newSession(t *testing.T, inName string) *testSession {

    ts := &testSession{
        name:inName,
        blocker:make(chan int, 100),
    }


    NaclProvider.StartSession(
        NaClProviderName,
        GatewayRWAccess,
        func(inSession Session, inErr *plan.Perror) {

            ts.session = inSession
            ts.blocker <- 1
        },
        nil,
    )

    <- ts.blocker
    identityResults, err := ts.doOp(
        OpArgs{
            OpName: OpNewIdentityRev,
        })
    if err != nil {
        t.Fatal(err)
    }


    ts.signingKeyID.AssignFrom( identityResults.GetContentWithLabel(PubSigningKeyName) )

    ts.encryptPubKey = identityResults.GetContentWithLabel(PubCryptoKeyName)
    ts.encryptPubKeyID.AssignFrom(ts.encryptPubKey)

    return ts
}