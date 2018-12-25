package main

import (
	"bytes"
    "math/rand"
    "fmt"
	"os/user"
    "path"
    //"time"
    "os"
    "crypto/md5"
    //"ioutil"

    "testing"

	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"

    "github.com/plan-systems/go-plan/ski/Providers/filesys"

	"github.com/plan-systems/go-plan/ski/CryptoKits/nacl"

)



var gTesting *testing.T
var gCommunityID = [4]byte{0, 1, 2, 3}
var gDefaulFileMode = os.FileMode(0775)

func getTmpDir(inSubDir string) string {

	usr, err := user.Current()
	if err != nil {
		gTesting.Fatal(err)
	}

	tmpDir := path.Join(usr.HomeDir, "_plan-testing")
    if len(inSubDir) > 0 {
        tmpDir = path.Join(tmpDir, inSubDir)
    }

    err = os.MkdirAll(tmpDir, gDefaulFileMode)
	if err != nil {
		gTesting.Fatal(err)
	}

    return tmpDir
}


func TestFileSysSKI(t *testing.T) {

    gTesting = t
   
    ski.RegisterProvider(filesys.Provider)
    ski.RegisterCryptoKit(&nacl.CryptoKit)

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
	
    // 2) generate a xfer community key msg from A
    opResults := A.doOp(ski.OpArgs{
        OpName: ski.OpExportNamedKeys,
        CryptoKey: ski.KeyEntry{
            KeyDomain: ski.KeyDomain_PERSONAL,
            PubKey: A.encryptPubKey,
        },
        KeySpecs: ski.KeyBundle{
            CommunityId: gCommunityID[:],
            Keys: []*ski.KeyEntry{
                &ski.KeyEntry{
                    KeyDomain: ski.KeyDomain_COMMUNITY,
                    PubKey: communityKey.PubKey,
                },
            },
        },
        PeerPubKey: B.encryptPubKey,
    })

    // 3) insert the new community key into B
    opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpImportKeys,
        Msg: opResults.Content,
        CryptoKey: ski.KeyEntry{
            KeyDomain: ski.KeyDomain_PERSONAL,
            PubKey: B.encryptPubKey,
        },
        KeySpecs: ski.KeyBundle{
            CommunityId: gCommunityID[:],
        },
        PeerPubKey: A.encryptPubKey,
    })


	clearMsg := []byte("hello, PLAN community!")

    // 4) Encrypt a new community msg (sent from A)
	opResults = A.doOp(ski.OpArgs{
        OpName: ski.OpEncrypt,
        CryptoKey: ski.KeyEntry{
            KeyDomain: ski.KeyDomain_COMMUNITY,
            PubKey: communityKey.PubKey,
        },
        KeySpecs: ski.KeyBundle{
            CommunityId: gCommunityID[:],
        },
        Msg: clearMsg,
    })

    encryptedMsg := opResults.Content

    // 5) Send the encrypted community message to B
	opResults = B.doOp(ski.OpArgs{
        OpName: ski.OpDecrypt,
        CryptoKey: ski.KeyEntry{
            KeyDomain: ski.KeyDomain_COMMUNITY,
            PubKey: communityKey.PubKey,
        },
        KeySpecs: ski.KeyBundle{
            CommunityId: gCommunityID[:],
        },
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

        _, opErr := B.doOpWithErr(ski.OpArgs{
            OpName: ski.OpDecrypt,
            CryptoKey: *communityKey,
            KeySpecs: ski.KeyBundle{
                CommunityId: gCommunityID[:],
            },
            Msg: badMsg,
        })
        if opErr == nil {
            gTesting.Fatal("there should have been a decryption error!")
        }
    }
}





type testSession struct {
    name        string
    session     ski.Session
    blocker     chan int   
    signingPubKey []byte
    encryptPubKey []byte
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

    ts.session.DispatchOp(&inOpArgs, func(opResults *plan.Block, inErr *plan.Perror) {
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
        blocker:make(chan int, 100),
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

    ts.encryptPubKey = ts.generateNewKey(ski.KeyType_ASYMMETRIC_KEY, ski.KeyDomain_PERSONAL).PubKey
    ts.signingPubKey = ts.generateNewKey(ski.KeyType_SIGNING_KEY,    ski.KeyDomain_PERSONAL).PubKey

    return ts
}

func (ts *testSession) generateNewKey(
    inKeyType ski.KeyType,
    inKeyDomain ski.KeyDomain,
) *ski.KeyEntry {

    var keyEntry *ski.KeyEntry

    ski.GenerateKeys(
        ts.session, 
        gCommunityID[:], 
        []*ski.KeyEntry{
            &ski.KeyEntry{
                KeyType: inKeyType,
                KeyDomain: inKeyDomain,
            },
        },
        func(inKeys []*ski.KeyEntry, inErr *plan.Perror) {
            if inErr != nil {
                gTesting.Fatal(inErr)
            } else {
                keyEntry = inKeys[0]
            }

            ts.blocker <- 1
        },
    )

    <- ts.blocker

    return keyEntry

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




