package ski // import "github.com/plan-tools/go-plan/ski"

import (
	"bytes"
	"testing"

	plan "github.com/plan-tools/go-plan/plan"
)

func TestSymmetricEncrypion(t *testing.T) {

	ski, _, _ := setUpSKI(t)
	keyID := ski.NewCommunityKey()
	clearIn := []byte("hello, world!")

	encryptOut, err := ski.Encrypt(keyID, clearIn)
	if err != nil {
		t.Fatal(err)
	}
	clearOut, err := ski.Decrypt(keyID, encryptOut)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(clearIn, clearOut) {
		t.Fatalf("got %v after decryption, expected %v", clearOut, clearIn)
	}
}

func TestPublicKeyEncrypion(t *testing.T) {

	senderSki, senderEncryptPubKey, _ := setUpSKI(t)
	recvSki, recvEncryptPubKey, _ := setUpSKI(t)
	clearIn := []byte("hello, world!")

	encryptOut, err := senderSki.EncryptFor(senderEncryptPubKey, clearIn, recvEncryptPubKey)
	if err != nil {
		t.Fatal(err)
	}
	clearOut, err := recvSki.DecryptFrom(
		recvEncryptPubKey, encryptOut, senderEncryptPubKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(clearIn, clearOut) {
		t.Fatalf("got %v after decryption, expected %v", clearOut, clearIn)
	}
}

func TestSigning(t *testing.T) {

	senderSki, _, senderSignPubKey := setUpSKI(t)
	recvSki, _, _ := setUpSKI(t)

	entry := &plan.PDIEntryCrypt{
		HeaderCrypt: []byte("encryptedtestheader"),
		BodyCrypt:   []byte("encryptedtestbody"),
    }
    
	hash := entry.ComputeHash()

	sig, err := senderSki.Sign(senderSignPubKey, hash)
	if err != nil {
		t.Fatal(err)
	}
	verified, ok := recvSki.Verify(senderSignPubKey, hash, sig)
	if !ok {
		t.Fatalf("signature verification failed: %x", verified)
	}
}

func TestVouching(t *testing.T) {

	senderSki, senderEncryptPubKey, _ := setUpSKI(t)
	recvSki, recvEncryptPubKey, _ := setUpSKI(t)
	keyID := senderSki.NewCommunityKey()

	msg, err := senderSki.Vouch(keyID, senderEncryptPubKey, recvEncryptPubKey)
	if err != nil {
		t.Fatal(err)
	}
	err = recvSki.AcceptVouch(recvEncryptPubKey, msg, senderEncryptPubKey)
	if err != nil {
		t.Fatal(err)
	}
}

// test setup helper
func setUpSKI(t *testing.T) (
	*LocalSKI, plan.IdentityPublicKey, plan.IdentityPublicKey,
) {
	ski := NewSKI()
	encryptPubKey, signPubKey := ski.NewIdentity()
	return ski, encryptPubKey, signPubKey
}
