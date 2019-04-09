package main

import (
	"bytes"
    "math/rand"
    //"fmt"
    //"time"
    //"ioutil"

    "testing"

	"github.com/plan-systems/go-plan/ski"
	//"github.com/plan-systems/go-plan/plan"

    "github.com/plan-systems/go-plan/ski/Providers/hive"

)


var gTesting *testing.T
var gCommunityID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

func TestFileSysSKI(t *testing.T) {

    gTesting = t

   {

        A := newSession("Alice")
        B := newSession("Bob")

        doProviderTest(A, B)

        A.EndSession("done A")
        B.EndSession("done B")
    }

}



func doProviderTest(A, B *testSession) {

    // 1) Generate a new community key (on A)
    err := A.GetLatestKey(&A.CommunityKey, ski.KeyType_SymmetricKey)
    if err != nil {
        gTesting.Fatal(err)
    }

    // 2) export the community key from A
    opBuf := A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_EXPORT_TO_PEER,
        OpKey: &A.P2PKey,
        PeerKey: B.P2PKey.PubKey,
        TomeIn: &ski.KeyTome{
            Keyrings: []*ski.Keyring{
                &ski.Keyring{
                    Name: A.CommunityKey.KeyringName,
                },
            },
        },
    })

    // 3) insert the new community key into B
    opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_IMPORT_FROM_PEER,
        BufIn: opBuf,
        OpKey: &B.P2PKey,
        PeerKey: A.P2PKey.PubKey,
    })


	clearMsg := []byte("hello, PLAN community!")

    // 4) Encrypt a new community msg (sent from A)
	opBuf = A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        BufIn: clearMsg,
        OpKey: &A.CommunityKey,
    })

    encryptedAtoB := opBuf

    // 5) Send the encrypted community message to B
	opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_DECRYPT_SYM,
        BufIn: encryptedAtoB,
        OpKey: &B.CommunityKey,
    })

    decryptedMsg := opBuf

    // 6) Now check that B can send an encrypted community msg to A
	opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        BufIn: decryptedMsg,
        OpKey: &B.CommunityKey,
    })
    encryptedBtoA := opBuf
	opBuf = A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_DECRYPT_SYM,
        BufIn: encryptedBtoA,
        OpKey: &A.CommunityKey,
    })

    // 7) Did the round trip work?
    //    clearMsg => A => encryptedAtoB => B => decryptedMsg => B => encryptedBtoA => A => opResults.Content
	if ! bytes.Equal(clearMsg, opBuf) {
		gTesting.Fatalf("expected %v, got %v after decryption", clearMsg, opBuf)
    }

    badMsg := make([]byte, len(encryptedAtoB))

    // Vary the data slightly to test 
    for i := 0; i < 1000; i++ {

        rndPos := rand.Int31n(int32(len(encryptedAtoB)))
        rndAdj := 1 + byte(rand.Int31n(254))
        copy(badMsg, encryptedAtoB)
        badMsg[rndPos] += rndAdj

        _, opErr := B.DoOp(ski.CryptOpArgs{
            CryptOp: ski.CryptOp_DECRYPT_SYM,
            BufIn: badMsg,
            OpKey: &B.CommunityKey,
        })
        if opErr == nil {
            gTesting.Fatal("there should have been a decryption error!")
        }
    }
}


type testSession struct {
    ski.SessionTool 
}




func (ts *testSession) doOp(inOpArgs ski.CryptOpArgs) []byte {

    results, err := ts.DoOp(inOpArgs)

    if err != nil {
        gTesting.Fatal(err)
    }
    return results
}







// test setup helper
func newSession(inUserName string) *testSession {

    session, err := hive.StartSession("", "test", nil)
    if err != nil {
        gTesting.Fatal(err)
    }

    tool, err := ski.NewSessionTool(
        session,
        inUserName,
        gCommunityID[:],
    )
    if err != nil {
        gTesting.Fatal(err)
    }

    ts := &testSession{
        *tool,
    }

    return ts
}




