// Package ed25519 uses the Ed25519 to implement sig-related parts of ski.CryptoKit.
// Calls into non-signature related CryptoKit functions will return an unimplemented err.
package ed25519

import (
	"io"

	"github.com/plan-systems/plan-go/ski"

	"golang.org/x/crypto/ed25519"
)

func init() {
	ski.RegisterCryptoKit(edKit{})
}

var unimpErr = ski.ErrCode_Unimplemented.ErrWithMsg("ed25519 only implements signature-related functionality")

type edKit struct {
}

func (kit edKit) CryptoKitID() ski.CryptoKitID {
	return ski.CryptoKitID_ED25519
}

func (kit edKit) GenerateNewKey(
	inRequestedKeySz int,
	ioRand io.Reader,
	ioEntry *ski.KeyEntry,
) error {
	var err error

	switch ioEntry.KeyInfo.KeyType {

	case ski.KeyType_SigningKey:
		{
			ioEntry.KeyInfo.PubKey, ioEntry.PrivKey, err = ed25519.GenerateKey(ioRand)
		}

	default:
		return ski.ErrCode_Unimplemented.ErrWithMsg("unimplemented KeyType")
	}

	if err != nil {
		return ski.ErrCode_KeyGenerationFailed.ErrWithMsgf("key generation failed for KeyType %v", ioEntry.KeyInfo.KeyType)
	}

	return nil
}

func (kit edKit) EncryptUsingPassword(
	ioRand io.Reader,
	inMsg []byte,
	inPwd []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) DecryptUsingPassword(
	inMsg []byte,
	inPwd []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) Encrypt(
	ioRand io.Reader,
	inMsg []byte,
	inKey []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) Decrypt(
	inMsg []byte,
	inKey []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) EncryptFor(
	ioRand       io.Reader,
	inMsg        []byte,
	inPeerPubKey []byte,
	inPrivKey    []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) DecryptFrom(
	inMsg        []byte,
	inPeerPubKey []byte,
	inPrivKey    []byte,
) ([]byte, error) {
	return nil, unimpErr
}

func (kit edKit) Sign(
	inDigest        []byte,
	inSignerPrivKey []byte,
) ([]byte, error) {
	if len(inSignerPrivKey) != ed25519.PrivateKeySize {
		return nil, ski.ErrCode_BadKeyFormat.ErrWithMsg("bad ed25519 private key size")
	}

	sig := ed25519.Sign(inSignerPrivKey, inDigest)
	return sig, nil
}

func (kit edKit) VerifySignature(
	inSig          []byte,
	inDigest       []byte,
	inSignerPubKey []byte,
) error {
	if len(inSignerPubKey) != ed25519.PublicKeySize {
		return ski.ErrCode_BadKeyFormat.ErrWithMsg("bad ed25519 public key size")
	}

	if !ed25519.Verify(inSignerPubKey, inDigest, inSig) {
		return ski.ErrCode_VerifySignatureFailed.ErrWithMsg("ed25519 sig verification failed")
	}
	return nil
}
