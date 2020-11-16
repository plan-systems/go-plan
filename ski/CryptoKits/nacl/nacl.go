// Package nacl uses (libSodium/naclKit) to implement ski.CryptoKit
package nacl

import (
	"io"

	"github.com/plan-systems/plan-go/ski"
	"golang.org/x/crypto/pbkdf2"

	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
	sign "golang.org/x/crypto/nacl/sign"
)

var (
	// An Alan Watts invocation....
	// Convenience for having zero data around
	zero64 = [64]byte{
		+0, +0, +0, +0, -0, -0, -0, -0,
		+0, -0, -0, +0, -0, -0, -0, -0,
		+0, -0, -0, +0, -0, -0, -0, -0,
		+0, +0, +0, +0, -0, -0, -0, -0,
		+0, +0, +0, +0, -0, -0, -0, -0,
		+0, +0, +0, +0, -0, +0, +0, -0,
		+0, +0, +0, +0, -0, +0, +0, -0,
		+0, +0, +0, +0, -0, -0, -0, -0,
	}
)

func init() {
	ski.RegisterCryptoKit(naclKit{})
}

type naclKit struct {
}

func (kit naclKit) CryptoKitID() ski.CryptoKitID {
	return ski.CryptoKitID_NaCl
}

/*****************************************************
** Symmetric encryption
**/

func (kit naclKit) Encrypt(
	ioRand io.Reader,
	inMsg  []byte,
	inKey  []byte,
) ([]byte, error) {

	if len(inKey) != 32 {
		return nil, ski.ErrCode_BadKeyFormat.ErrWithMsgf("unexpected key size, want %v, got %v", 32, len(inKey))
	}

	var salt [24]byte
	_, err := ioRand.Read(salt[:])
	if err != nil {
		return nil, err
	}

	var privKey [32]byte
	copy(privKey[:], inKey[:32])

	msg := secretbox.Seal(salt[:], inMsg, &salt, &privKey)

	// Don't leave any private key bytes in memory
	copy(privKey[:], zero64[:32])

	return msg, nil
}

func (kit naclKit) Decrypt(
	inMsg []byte,
	inKey []byte,
) ([]byte, error) {
	var salt [24]byte
	copy(salt[:], inMsg[:24])

	var privKey [32]byte
	copy(privKey[:], inKey[:32])

	var err error
	msg, ok := secretbox.Open(nil, inMsg[24:], &salt, &privKey)
	if ok == false {
		err = ski.ErrCode_DecryptFailed.ErrWithMsg("secretbox.Open failed to decrypt data")
	}

	// Don't leave any private key bytes in memory
	copy(privKey[:], zero64[:32])

	return msg, err
}

/*****************************************************
** Key generation
**/

func (kit naclKit) GenerateNewKey(
	inRequestedKeySz int,
	ioRand           io.Reader,
	ioEntry          *ski.KeyEntry,
) error {
	var err error

	keyInfo := ioEntry.KeyInfo
	switch keyInfo.KeyType {

	case ski.KeyType_SymmetricKey:
		{
			keyInfo.PubKey = make([]byte, inRequestedKeySz)
			_, err = ioRand.Read(keyInfo.PubKey)
			if err == nil {
				ioEntry.PrivKey = make([]byte, 32)
				_, err = ioRand.Read(ioEntry.PrivKey)
			}
		}

	case ski.KeyType_AsymmetricKey:
		{
			var pubKey, privKey *[32]byte
			pubKey, privKey, err = box.GenerateKey(ioRand)
			if err == nil {
				keyInfo.PubKey = pubKey[:]
				ioEntry.PrivKey = privKey[:]
			}
		}

	case ski.KeyType_SigningKey:
		{
			var privKey *[64]byte
			var pubKey *[32]byte
			pubKey, privKey, err = sign.GenerateKey(ioRand)
			if err == nil {
				keyInfo.PubKey = pubKey[:]
				ioEntry.PrivKey = privKey[:]
			}
		}

	default:
		return ski.ErrCode_Unimplemented.ErrWithMsg("unknown KeyType")
	}

	if err != nil {
		return ski.ErrCode_KeyGenerationFailed.ErrWithMsgf("key generation failed for KeyType %v", keyInfo.KeyType)
	}

	return nil
}

func (kit naclKit) EncryptUsingPassword(
	ioRand io.Reader,
	inMsg  []byte,
	inPwd  []byte,
) ([]byte, error) {

	hashKitID := ski.HashKitID_Blake2b_256
	hasher := ski.FetchHasher(hashKitID)

	var salt [26]byte
	salt[0] = byte(hashKitID)
	salt[1] = 0
	ioRand.Read(salt[2:])

	privKey := pbkdf2.Key(inPwd, salt[2:], 4096, 32, hasher)

	buf, err := kit.Encrypt(ioRand, inMsg, privKey)
	if err != nil {
		return nil, err
	}

	return append(salt[:], buf...), nil
}

func (kit naclKit) DecryptUsingPassword(
	inMsg []byte,
	inPwd []byte,
) ([]byte, error) {

	hashKitID := ski.HashKitID(inMsg[0])
	hasher := ski.FetchHasher(hashKitID)
	if hasher == nil {
		return nil, ski.ErrCode_HashKitNotFound.ErrWithMsgf("HashKitID %v not recognized", hashKitID)
	}

	privKey := pbkdf2.Key(inPwd, inMsg[2:26], 4096, 32, hasher)

	return kit.Decrypt(inMsg[26:], privKey)
}

/*****************************************************
** Asymmetric encryption
**/

func (kit naclKit) EncryptFor(
	ioRand       io.Reader,
	inMsg        []byte,
	inPeerPubKey []byte,
	inPrivKey    []byte,
) ([]byte, error) {

	if len(inPeerPubKey) != 32 {
		return nil, ski.ErrCode_BadKeyFormat.ErrWithMsgf("unexpected peer pub key length, want %v, got %v", 32, len(inPeerPubKey))
	}

	if len(inPrivKey) != 32 {
		return nil, ski.ErrCode_BadKeyFormat.ErrWithMsgf("unexpected private key length, want %v got %v", 32, len(inPrivKey))
	}

	var salt [24]byte
	_, err := ioRand.Read(salt[:])
	if err != nil {
		return nil, err
	}

	var privKey, peerPubKey [32]byte
	copy(peerPubKey[:], inPeerPubKey[:32])
	copy(privKey[:], inPrivKey[:32])

	msg := box.Seal(salt[:], inMsg, &salt, &peerPubKey, &privKey)

	// Don't leave any private key bytes in memory
	copy(privKey[:], zero64[:32])

	return msg, nil
}

func (kit naclKit) DecryptFrom(
	inMsg        []byte,
	inPeerPubKey []byte,
	inPrivKey    []byte,
) ([]byte, error) {

	var salt [24]byte
	copy(salt[:], inMsg[:24])

	var privKey, peerPubKey [32]byte
	copy(peerPubKey[:], inPeerPubKey[:32])
	copy(privKey[:], inPrivKey[:32])

	var err error
	msg, ok := box.Open(nil, inMsg[24:], &salt, &peerPubKey, &privKey)
	if !ok {
		err = ski.ErrCode_DecryptFailed.ErrWithMsgf("NaCl failed to decrypt for peer %v", inPeerPubKey)
	}

	// Don't leave any private key bytes in memory
	copy(privKey[:], zero64[:32])

	return msg, err
}

/*****************************************************
** Signing & Verification
**/

func (kit naclKit) Sign(
	inDigest        []byte,
	inSignerPrivKey []byte,
) ([]byte, error) {

	if len(inSignerPrivKey) != 64 {
		return nil, ski.ErrCode_BadKeyFormat.ErrWithMsgf("bad signing key size, got %v", len(inSignerPrivKey))
	}

	var privKey [64]byte
	copy(privKey[:], inSignerPrivKey[:64])

	sig := sign.Sign(nil, inDigest, &privKey)

	// Don't leave any private key bytes in memory
	copy(privKey[:], zero64[:64])

	return sig[:sign.Overhead], nil
}

func (kit naclKit) VerifySignature(
	inSig          []byte,
	inDigest       []byte,
	inSignerPubKey []byte,
) error {

	// need to re-combine the sig and hash to produce the
	// signed message that Open expects
	signedMsg := make([]byte, 0, len(inSig)+len(inDigest))
	signedMsg = append(signedMsg, inSig...)
	signedMsg = append(signedMsg, inDigest...)

	var pubKey [32]byte
	copy(pubKey[:], inSignerPubKey[:32])

	_, ok := sign.Open(nil, signedMsg, &pubKey)

	if !ok {
		return ski.ErrCode_VerifySignatureFailed.ErrWithMsg("NaCl sig verification failed")
	}

	return nil
}
