package main

import (
	"bytes"
    "math/rand"
    "fmt"
    //"time"
    //"ioutil"

    "testing"

	"github.com/plan-systems/go-plan/ski"
	//"github.com/plan-systems/go-plan/plan"

    "github.com/plan-systems/go-plan/ski/Providers/hive"

)


var gTesting *testing.T
var gCommunityID = [8]byte{0, 1, 2, 3, 4, 5, 6, 7}

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

        fmt.Printf("%s's encryptKey %v\n", A.UserName, A.encryptKey)
        fmt.Printf("%s's encryptKey %v\n", B.UserName, B.encryptKey)

        doProviderTest(A, B)

        A.EndSession("done A")
        B.EndSession("done B")
    }

}



func doProviderTest(A, B *testSession) {

    // 1) Generate a new community key (on A)
    communityKeyRef, err := A.GenerateNewKey(ski.KeyType_SYMMETRIC_KEY, A.CommunityKey.KeyringName)
    if err != nil {
        gTesting.Fatal(err)
    }

    // 2) export the community key from A
    opBuf := A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_EXPORT_TO_PEER,
        OpKey: A.encryptKey,
        PeerPubKey: B.encryptKey.PubKey,
        TomeIn: &ski.KeyTome{
            Keyrings: []*ski.Keyring{
                &ski.Keyring{
                    Name: communityKeyRef.KeyringName,
                },
            },
        },
    })

    // 3) insert the new community key into B
    opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_IMPORT_FROM_PEER,
        BufIn: opBuf,
        OpKey: B.encryptKey,
        PeerPubKey: A.encryptKey.PubKey,
    })


	clearMsg := []byte("hello, PLAN community!")

    // 4) Encrypt a new community msg (sent from A)
	opBuf = A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        BufIn: clearMsg,
        OpKey: communityKeyRef,
    })

    encryptedAtoB := opBuf

    // 5) Send the encrypted community message to B
	opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_DECRYPT_SYM,
        BufIn: encryptedAtoB,
        OpKey: communityKeyRef,
    })

    decryptedMsg := opBuf

    // 6) Now check that B can send an encrypted community msg to A
	opBuf = B.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        BufIn: decryptedMsg,
        OpKey: communityKeyRef,
    })
    encryptedBtoA := opBuf
	opBuf = A.doOp(ski.CryptOpArgs{
        CryptOp: ski.CryptOp_DECRYPT_SYM,
        BufIn: encryptedBtoA,
        OpKey: communityKeyRef,
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
            OpKey: communityKeyRef,
        })
        if opErr == nil {
            gTesting.Fatal("there should have been a decryption error!")
        }
    }
}


type testSession struct {
    ski.SessionTool 

    encryptKey *ski.KeyRef
}




func (ts *testSession) doOp(inOpArgs ski.CryptOpArgs) []byte {

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

    userName := []byte(inUserName)

    ts.encryptKey, err = ts.GenerateNewKey(
        ski.KeyType_ASYMMETRIC_KEY, 
        append(gCommunityID[:], userName...),
    )

    return ts
}




