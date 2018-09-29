package main

import (
	"bytes"
	crypto_rand "crypto/rand"
	math_rand "math/rand"

	"testing"

	"github.com/plan-tools/go-plan/ski"

	"github.com/plan-tools/go-plan/ski/CryptoPkgs/nacl"
)

var gTesting *testing.T

func TestCryptoPkgs(t *testing.T) {

	gTesting = t

	// Register providers to test
	cryptoPkgsToTest := []*ski.CryptoPkg{
		&nacl.CryptoPkg,
	}

	for _, pkg := range cryptoPkgsToTest {

		for i := 0; i < 100; i++ {
			testPkg(pkg, 32)
		}
	}
}

func testPkg(pkg *ski.CryptoPkg, inKeyLen int) {

	msgSz := int(1 + math_rand.Int31n(5) + 7 * math_rand.Int31n(10))

	msg := make([]byte, msgSz)
	msgOrig := make([]byte, msgSz)
	badMsg := make([]byte, msgSz)

	crypto_rand.Read(msg)
	copy(msgOrig, msg)

	reader := crypto_rand.Reader

	if bytes.Compare(msgOrig, msg) != 0 {
		gTesting.Fatal("initial msg check failed!?")
	}

	entry := ski.KeyEntry{}

	var crypt []byte

	/*****************************************************
	** Symmetric test
	**/

	{

		entry.KeyInfo = ski.EncodeKeyInfo(pkg.CryptoPkgID, ski.KeyType_SYMMETRIC_KEY)
		err := pkg.GenerateNewKey(reader, inKeyLen, &entry)
		if err != nil {
			gTesting.Fatal(err)
		}

		crypt, err = pkg.Encrypt(reader, msgOrig, entry.PrivKey)
		if err != nil {
			gTesting.Fatal(err)
		}

		if len(badMsg) != len(crypt) {
			badMsg = make([]byte, len(crypt))
		}

		msg, err = pkg.Decrypt(crypt, entry.PrivKey)
		if bytes.Compare(msg, msgOrig) != 0 {
			gTesting.Fatal("symmetric decrypt failed check")
		}

		// Vary the data slightly to test
		for k := 0; k < 100; k++ {

			rndPos := math_rand.Int31n(int32(len(crypt)))
			rndAdj := 1 + byte(math_rand.Int31n(254))
			copy(badMsg, crypt)
			badMsg[rndPos] += rndAdj

			msg, err = pkg.Decrypt(badMsg, entry.PrivKey)
			if err == nil {
				gTesting.Fatal("there should have been a decryption error!")
			}
		}
	}

	/*****************************************************
	** Asymmetric test
	**/

	{

		entry.KeyInfo = ski.EncodeKeyInfo(pkg.CryptoPkgID, ski.KeyType_ASYMMETRIC_KEY)
		err := pkg.GenerateNewKey(reader, inKeyLen, &entry)
		if err != nil {
			gTesting.Fatal(err)
		}

		recipient := ski.KeyEntry{}
		recipient.KeyInfo = ski.EncodeKeyInfo(pkg.CryptoPkgID, ski.KeyType_ASYMMETRIC_KEY)
		err = pkg.GenerateNewKey(reader, inKeyLen, &recipient)
		if err != nil {
			gTesting.Fatal(err)
		}

		crypt, err = pkg.EncryptFor(reader, msgOrig, recipient.PubKey, entry.PrivKey)
		if err != nil {
			gTesting.Fatal(err)
		}

		if len(badMsg) != len(crypt) {
			badMsg = make([]byte, len(crypt))
		}

		msg, err = pkg.DecryptFrom(crypt, entry.PubKey, recipient.PrivKey)
		if bytes.Compare(msg, msgOrig) != 0 {
			gTesting.Fatal("asymmetric decrypt failed check")
		}

		// Vary the data slightly to test
		for k := 0; k < 100; k++ {

			rndPos := math_rand.Int31n(int32(len(crypt)))
			rndAdj := 1 + byte(math_rand.Int31n(254))
			copy(badMsg, crypt)
			badMsg[rndPos] += rndAdj

			msg, err = pkg.DecryptFrom(badMsg, entry.PubKey, recipient.PrivKey)
			if err == nil {
				gTesting.Fatal("there should have been a decryption error!")
			}
		}

	}

	/*****************************************************
	** Signing test
	**/

	{

		entry.KeyInfo = ski.EncodeKeyInfo(pkg.CryptoPkgID, ski.KeyType_SIGNING_KEY)
		err := pkg.GenerateNewKey(reader, inKeyLen, &entry)
		if err != nil {
			gTesting.Fatal(err)
		}

		crypt, err = pkg.Sign(msgOrig, entry.PrivKey)
		if err != nil {
			gTesting.Fatal(err)
		}

		if len(badMsg) != len(crypt) {
			badMsg = make([]byte, len(crypt))
		}

		err = pkg.VerifySignature(crypt, msgOrig, entry.PubKey)
		if err != nil {
			gTesting.Fatal(err)
		}

		// Vary the data slightly to test
		for k := 0; k < 100; k++ {

			rndPos := math_rand.Int31n(int32(len(crypt)))
			rndAdj := 1 + byte(math_rand.Int31n(254))
			copy(badMsg, crypt)
			badMsg[rndPos] += rndAdj

			err = pkg.VerifySignature(badMsg, msgOrig, entry.PubKey)
			if err == nil {
				gTesting.Fatal("there should have been a sig failed error!")
			}
		}

	}

}
