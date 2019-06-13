// Package nacl uses (libSodium/NaCl) to implement ski.CryptoKit
package nacl

import (
    //"encoding/json"
    //"net/http"
    //"log"
    "io"
	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"
    
    "golang.org/x/crypto/pbkdf2"

	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
    sign "golang.org/x/crypto/nacl/sign"
)

var (
    // An Alan Watts invocation....
    // Convenience for having zero data around
    zero64 = [64]byte{
        +0,+0,+0,+0,-0,-0,-0,-0,
        +0,-0,-0,+0,-0,-0,-0,-0,
        +0,-0,-0,+0,-0,-0,-0,-0,
        +0,+0,+0,+0,-0,-0,-0,-0,
        +0,+0,+0,+0,-0,-0,-0,-0,
        +0,+0,+0,+0,-0,+0,+0,-0,
        +0,+0,+0,+0,-0,+0,+0,-0,
        +0,+0,+0,+0,-0,-0,-0,-0,
    }
)

func init() {
    ski.RegisterCryptoKit(&CryptoKit)
}

func symEncrypt(
    inRand io.Reader, 
    inMsg []byte,
    inKey []byte,
) ([]byte, error) {

    if len(inKey) != 32 {
        return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected key size, want %v, got %v", 32, len(inKey))
    }

    var salt [24]byte
    _, err := inRand.Read(salt[:])
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

func symDecrypt(
    inMsg []byte,
    inKey []byte,
) ([]byte, error) {

    var salt [24]byte
    copy(salt[:], inMsg[:24])
    
    var privKey [32]byte
    copy(privKey[:], inKey[:32])

    var err error
    msg, ok := secretbox.Open(nil, inMsg[24:], &salt, &privKey)
    if ! ok {
        err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt data")
    }

    // Don't leave any private key bytes in memory
    copy(privKey[:], zero64[:32])


    return msg, err
}

// CryptoKit is used with ski.RegisterCryptoKit() so it is freely available.
var CryptoKit = ski.CryptoKit{

    CryptoKitID: ski.CryptoKitID_NaCl,

	/*****************************************************
	** Key generation
	**/

    GenerateNewKey: func(
        inRand io.Reader,
        inRequestedKeyLen int,
        ioEntry *ski.KeyEntry,
    ) error {

        var err error

        keyInfo := ioEntry.KeyInfo
        switch keyInfo.KeyType {

            case ski.KeyType_SymmetricKey: {
                keyInfo.PubKey = make([]byte, plan.SymmetricPubKeySz)
                _, err = inRand.Read(keyInfo.PubKey)
                if err == nil {
                    ioEntry.PrivKey = make([]byte, 32)
                    _, err = inRand.Read(ioEntry.PrivKey)
                }
            }
        
            case ski.KeyType_AsymmetricKey: {
                var pubKey, privKey *[32]byte
                pubKey, privKey, err = box.GenerateKey(inRand)
                if err == nil {
                    keyInfo.PubKey = pubKey[:]
                    ioEntry.PrivKey = privKey[:]
                }
            }

            case ski.KeyType_SigningKey: {
                var privKey *[64]byte
                var pubKey *[32]byte
                pubKey, privKey, err = sign.GenerateKey(inRand)
                if err == nil {
                    keyInfo.PubKey = pubKey[:]
                    ioEntry.PrivKey = privKey[:]
                }
            }

            default:
                return plan.Error(nil, plan.Unimplemented, "unknown KeyType")
        }

        if err != nil {
            return plan.Errorf(err, plan.KeyGenerationFailed, "key generation failed for KeyType %v", keyInfo.KeyType)
        }

        return nil
    },

    EncryptUsingPassword: func(
        inRand io.Reader, 
        inMsg []byte,
        inPwd []byte,
    ) ([]byte, error) {

        hashKitID := ski.HashKitID_Blake2b_256
        hasher := ski.FetchHasher(hashKitID)

        var salt [26]byte
        salt[0] = byte(hashKitID)
        salt[1] = 0
        inRand.Read(salt[2:])

        privKey := pbkdf2.Key(inPwd, salt[2:], 4096, 32, hasher)

        buf, err := symEncrypt(inRand, inMsg, privKey)
        if err != nil {
            return nil, err
        }

        return append(salt[:], buf...), nil
    },

    DecryptUsingPassword: func(
        inMsg []byte,
        inPwd []byte,
    ) ([]byte, error) {

        hashKitID := ski.HashKitID(inMsg[0])
        hasher := ski.FetchHasher(hashKitID)
        if hasher == nil {
            return nil, plan.Errorf(nil, plan.HashKitNotFound, "failed to recognize HashKitID %v", hashKitID)
        }

        privKey := pbkdf2.Key(inPwd, inMsg[2:26], 4096, 32, hasher)

        return symDecrypt(inMsg[26:], privKey)
    },

	/*****************************************************
	** Symmetric encryption
	**/

    Encrypt: symEncrypt,

    Decrypt: symDecrypt,

	/*****************************************************
	** Asymmetric encryption
	**/

    EncryptFor: func(
        inRand io.Reader, 
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {

        if len(inPeerPubKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected peer pub key length, want %v, got %v", 32, len(inPeerPubKey))
        }

        if len(inPrivKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected private key length, want %v got %v", 32, len(inPrivKey))
        }

        var salt [24]byte
        _, err := inRand.Read(salt[:])
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
    },

    DecryptFrom: func(
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {

        var salt [24]byte
        copy(salt[:], inMsg[:24])
        
        var privKey, peerPubKey [32]byte
        copy(peerPubKey[:], inPeerPubKey[:32])
        copy(privKey[:], inPrivKey[:32])

        var err error
        msg, ok := box.Open(nil, inMsg[24:], &salt, &peerPubKey, &privKey)
        if ! ok {
            err = plan.Errorf(nil, plan.FailedToDecryptData, "NaCl failed to decrypt for peer %v", inPeerPubKey)
        }

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])

        return msg, err
    },

	/*****************************************************
	** Signing & Verification
	**/

    Sign: func(
        inDigest []byte,
        inSignerPrivKey []byte,
    ) ([]byte, error) {

        if len(inSignerPrivKey) != 64 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "bad signing key size, got %v", len(inSignerPrivKey))
        }

        var privKey [64]byte
        copy(privKey[:], inSignerPrivKey[:64])

        sig := sign.Sign(nil, inDigest, &privKey)

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:64])

        return sig[:sign.Overhead], nil
    },

    VerifySignature: func(
        inSig []byte,
        inDigest []byte,
        inSignerPubKey []byte,
    ) error {

        // need to re-combine the sig and hash to produce the
        // signed message that Open expects
        signedMsg := make([]byte, 0, len(inSig) + len(inDigest))
        signedMsg = append(signedMsg, inSig...)
        signedMsg = append(signedMsg, inDigest...)
        
        var pubKey [32]byte
        copy(pubKey[:], inSignerPubKey[:32])

        _, ok := sign.Open(nil, signedMsg, &pubKey)

        if ! ok {
            return plan.Error(nil, plan.VerifySignatureFailed, "NaCl sig verification failed")
        }
                
        return nil
    },
}
