package ski // import "github.com/plan-tools/go-plan/ski"

import (
	"bytes"
    "math/rand"
    "fmt"

    "testing"

	plan "github.com/plan-tools/go-plan/plan"
)






func TestCommunityEncryption(t *testing.T) {

    A := newSession(t, "Alice")
    B := newSession(t, "Bob")

    // 1) make a new community key
	err, opResults := A.doOp(OpArgs{
        OpName: OpCreateCommunityKey,
    })
	if err != nil {
		t.Fatal(err)
    }
    var communityKeyID plan.KeyID
    communityKeyID.AssignFrom(opResults[0].buf)

    fmt.Printf("%s's encryptPubKey %v\n", A.name, A.encryptPubKey)
    fmt.Printf("%s's encryptPubKey %v\n", B.name, B.encryptPubKey)

    // 2) generate a xfer community key msg from A
    err, opResults = A.doOp(OpArgs{
        OpName: OpSendCommunityKeys,
        OpKeyIDs: []plan.KeyID{communityKeyID},
        PeerPubKey: B.encryptPubKey,
        CryptoKeyID: A.encryptPubKeyID,
    })
    if err != nil {
        t.Fatal(err)
    }
    
    // 3) insert the new community key into B
    err, opResults = B.doOp(OpArgs{
        OpName: OpAcceptCommunityKeys,
        Msg: opResults[0].buf,
        PeerPubKey: A.encryptPubKey,
        CryptoKeyID: B.encryptPubKeyID,
    })
    if err != nil {
        t.Fatal(err)
    }

	clearMsg := []byte("hello, PLAN community!")

    // Encrypt a new communuty msg on A
	err, opResults = A.doOp(OpArgs{
        OpName: OpEncryptForCommunity,
        CryptoKeyID: communityKeyID,
        Msg: clearMsg,
    })
	if err != nil {
		t.Fatal(err)
	}

    encryptedMsg := opResults[0].buf

    // Send the encypted community message to B
	err, opResults = B.doOp(OpArgs{
        OpName: OpDecryptFromCommunity,
        CryptoKeyID: communityKeyID,
        Msg: encryptedMsg,
    })
	if err != nil {
		t.Fatal(err)
    }

	if ! bytes.Equal(clearMsg, opResults[0].buf) {
		t.Fatalf("expected %v, got %v after decryption", clearMsg, opResults[0].buf )
    }
    

    badMsg := make([]byte, len(encryptedMsg))

    // Vary the data slightly to test 
    for i := 0; i < 1000; i++ {

        rndPos := rand.Int31n(int32(len(encryptedMsg)))
        rndAdj := 1 + byte(rand.Int31n(254))
        copy(badMsg, encryptedMsg)
        badMsg[rndPos] += rndAdj

        err, _ = B.doOp(OpArgs{
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




func (ts *testSession) doOp(inOpArgs OpArgs) (*plan.Perror, []OpResult) {

    var outErr *plan.Perror
    var outResults []OpResult

    ts.session.DispatchOp(&inOpArgs, func(inErr *plan.Perror, opResults []OpResult){
        outErr = inErr
        outResults = opResults

        ts.blocker <- 1
    })

    <- ts.blocker

    return outErr, outResults
}





// test setup helper
func newSession(t *testing.T, inName string) *testSession {

    ts := &testSession{
        name:inName,
        blocker:make(chan int, 100),
    }


    NaclProvider.StartSession(
        InvokeNaCl,
        GatewayRWAccess,
        func(inErr *plan.Perror, inSession Session) {

            ts.session = inSession
            ts.blocker <- 1
        },
        nil,
    )

    <- ts.blocker
    err, identityResults := ts.doOp(
        OpArgs{
            OpName: OpNewIdentityRev,
        })
    if err != nil {
        t.Fatal(err)
    }
    if len(identityResults) < 2 {
        t.Fatal("identityResults < 2")
    }


    ts.signingKeyID.AssignFrom(identityResults[0].buf)

    ts.encryptPubKey = identityResults[1].buf
    ts.encryptPubKeyID.AssignFrom(identityResults[1].buf)

    return ts
}