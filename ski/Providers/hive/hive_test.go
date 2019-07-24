package hive


import (
	"io/ioutil"
    "errors"
	"bytes"
	"math/rand"
	"testing"



	"github.com/plan-systems/plan-core/ski"

	//"github.com/plan-systems/plan-core/ski/hive"

	//"github.com/plan-systems/plan-core/ski/CryptoKits/nacl"
	//"github.com/plan-systems/plan-core/ski/CryptoKits/ed25519"
)

var gTesting *testing.T



func TestFileStorage(t *testing.T) {

	gTesting = t

	baseDir, err := ioutil.TempDir("", "hive_test")
	if err != nil {
		gTesting.Fatal(err)
	}

    pass := make([]byte, 4 + rand.Intn(7))
    N, err := rand.Read(pass)
    if err != nil || N != len(pass) {
        if err == nil {
            err = errors.New("rand is broken?")
        }
        gTesting.Fatal(err)
    }

    keyRef := ski.KeyRef{
        KeyringName: make([]byte, 8 + rand.Intn(16)),
    }
    rand.Read(keyRef.KeyringName)

    sess, err := StartSession(baseDir, "hive-test", pass)
	if err != nil {
		gTesting.Fatal(err)
	}
    
    keyInfo, err := ski.GenerateNewKey(
        sess,
        keyRef.KeyringName,
        ski.KeyInfo{
            KeyType: ski.KeyType_SymmetricKey,
        },
    )
    keyRef.PubKey = keyInfo.PubKey

    testMsgIn := make([]byte, 50 + rand.Intn(500))
    rand.Read(testMsgIn)

    out, err := sess.DoCryptOp(
        &ski.CryptOpArgs{
            CryptOp: ski.CryptOp_ENCRYPT_SYM,
            OpKey: &keyRef,
            BufIn: testMsgIn,
        },
    )
    testMsgEnc := out.BufOut
	if err != nil {
		gTesting.Fatal(err)
	}

    sess.EndSession("end test sess (should trigger save to disk")

    sess, err = StartSession(baseDir, "hive-test", pass[:len(pass)-2])
	if err == nil {
		gTesting.Fatal("should have gotten an error from bad pass")
	}

    sess, err = StartSession(baseDir, "hive-test", pass)
	if err != nil {
		gTesting.Fatal(err)
	}

    out, err = sess.DoCryptOp(
        &ski.CryptOpArgs{
            CryptOp: ski.CryptOp_DECRYPT_SYM,
            OpKey: &keyRef,
            BufIn: testMsgEnc,
        },
    )
	if err != nil {
		gTesting.Fatal(err)
	}

    if !bytes.Equal(out.BufOut, testMsgIn) {
        gTesting.Fatal("test msg didn't match")
    }


}

