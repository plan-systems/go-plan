// Package nacl uses (libSodium/NaCl) to implement ski.CryptoKit
package nacl

import (
    //"encoding/json"
    //"net/http"
    //"log"
    "io"
	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"
    
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
    ) *plan.Err {

        var err error

        switch ioEntry.KeyType {

            case ski.KeyType_SYMMETRIC_KEY: {
                ioEntry.PubKey = make([]byte, inRequestedKeyLen)
                _, err = inRand.Read(ioEntry.PubKey)
                if err == nil {
                    ioEntry.PrivKey = make([]byte, 32)
                    _, err = inRand.Read(ioEntry.PrivKey)
                }

            }
        
            case ski.KeyType_ASYMMETRIC_KEY: {
                pubKey, privKey, err := box.GenerateKey(inRand)
                if err == nil {
                    ioEntry.PubKey = pubKey[:]
                    ioEntry.PrivKey = privKey[:]
                }
            }

            case ski.KeyType_SIGNING_KEY: {
                pubKey, privKey, err := sign.GenerateKey(inRand)
                if err == nil {
                    ioEntry.PubKey = pubKey[:]
                    ioEntry.PrivKey = privKey[:]
                }
            }

            default:
                return plan.Errorf(nil, plan.KeyGenerationFailed, "unrecognized key type KeyType: %v}", ioEntry.KeyType)
        }

        if err != nil {
            return plan.Errorf(err, plan.KeyGenerationFailed, "key generation failed {KeyType: %v}", ioEntry.KeyType)
        }

        return nil
    },

	/*****************************************************
	** Symmetric encryption
	**/

    Encrypt: func(
        inRand io.Reader, 
        inMsg []byte,
        inKey []byte,
    ) ([]byte, *plan.Err) {

        if len(inKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected key size, want %v, got %v", 32, len(inKey))
        }

        var salt [24]byte
        inRand.Read(salt[:])

        var privKey [32]byte
        copy(privKey[:], inKey[:32])
        
        msg := secretbox.Seal(salt[:], inMsg, &salt, &privKey)

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])

        return msg, nil
    },


    Decrypt: func(
        inMsg []byte,
        inKey []byte,
    ) ([]byte, *plan.Err) {

        var salt [24]byte
        copy(salt[:], inMsg[:24])
        
        var privKey [32]byte
        copy(privKey[:], inKey[:32])

        var err *plan.Err
        msg, ok := secretbox.Open(nil, inMsg[24:], &salt, &privKey)
        if ! ok {
            err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt data")
        }

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])


        return msg, err
    },

	/*****************************************************
	** Asymmetric encryption
	**/

    EncryptFor: func(
        inRand io.Reader, 
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, *plan.Err) {

        if len(inPeerPubKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected peer pub key length, want %v, got %v", 32, len(inPeerPubKey))
        }

        if len(inPrivKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected private key length, want %v got %v", 32, len(inPrivKey))
        }

        var salt [24]byte
        inRand.Read(salt[:])

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
    ) ([]byte, *plan.Err) {

        var salt [24]byte
        copy(salt[:], inMsg[:24])
        
        var privKey, peerPubKey [32]byte
        copy(peerPubKey[:], inPeerPubKey[:32])
        copy(privKey[:], inPrivKey[:32])

        var err *plan.Err
        msg, ok := box.Open(nil, inMsg[24:], &salt, &peerPubKey, &privKey)
        if ! ok {
            err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt for peer %v", inPeerPubKey)
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
    ) ([]byte, *plan.Err) {

        if len(inSignerPrivKey) != 64 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected sign key size, want %v, got %v", 64, len(inSignerPrivKey))
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
    ) *plan.Err {

        // need to re-combine the sig and hash to produce the
        // signed message that Open expects
        signedMsg := make([]byte, 0, len(inSig) + len(inDigest))
        signedMsg = append(signedMsg, inSig...)
        signedMsg = append(signedMsg, inDigest...)
        
        var pubKey [32]byte
        copy(pubKey[:], inSignerPubKey[:32])

        _, ok := sign.Open(nil, signedMsg, &pubKey)

        if !ok {
            return plan.Error(nil, plan.VerifySignatureFailed, "nacl sig verification failed")
        }
                
        return nil
        
    },

}


 
