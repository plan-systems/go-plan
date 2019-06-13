// Package ed25519 uses the Ed25519 to implement sig-related parts of ski.CryptoKit.
// Calls into non-signature related CryptoKit functions will return unimplemented err.
package ed25519

import (
    //"encoding/json"
    //"net/http"
    //"log"
    "io"
	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"
    
    //"golang.org/x/crypto/pbkdf2"

	"golang.org/x/crypto/ed25519"
)

func init() {
    ski.RegisterCryptoKit(&CryptoKit)
}

var unimpErr = plan.Errorf(nil, plan.Unimplemented, "ed25519 only implements signature-related functionality")

// CryptoKit is used with ski.RegisterCryptoKit() so it can be accessed by ski.CryptoKitID
var CryptoKit = ski.CryptoKit{

    CryptoKitID: ski.CryptoKitID_ED25519,

	/*****************************************************
	** Key generation
	**/

    GenerateNewKey: func(
        inRand io.Reader,
        inRequestedKeyLen int,
        ioEntry *ski.KeyEntry,
    ) error {
        
        var err error 

        switch ioEntry.KeyInfo.KeyType {

            case ski.KeyType_SigningKey: {
                ioEntry.KeyInfo.PubKey, ioEntry.PrivKey, err = ed25519.GenerateKey(inRand)
            }

            default:
                return plan.Error(nil, plan.Unimplemented, "unimplemented KeyType")
        }

        if err != nil {
            return plan.Errorf(err, plan.KeyGenerationFailed, "key generation failed for KeyType %v", ioEntry.KeyInfo.KeyType)
        }

        return nil
    },

    EncryptUsingPassword: func(
        inRand io.Reader, 
        inMsg []byte,
        inPwd []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

    DecryptUsingPassword: func(
        inMsg []byte,
        inPwd []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

	/*****************************************************
	** Symmetric encryption
	**/

    Encrypt: func(
        inRand io.Reader, 
        inMsg []byte,
        inKey []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

    Decrypt: func(
        inMsg []byte,
        inKey []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

	/*****************************************************
	** Asymmetric encryption
	**/

    EncryptFor: func(
        inRand io.Reader, 
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

    DecryptFrom: func(
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {
        return nil, unimpErr
    },

	/*****************************************************
	** Signing & Verification
	**/

    Sign: func(
        inDigest []byte,
        inSignerPrivKey []byte,
    ) ([]byte, error) {

        if len(inSignerPrivKey) != ed25519.PrivateKeySize {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "bad ed25519 private key size")
        }

        sig := ed25519.Sign(inSignerPrivKey, inDigest)

        return sig, nil
    },

    VerifySignature: func(
        inSig []byte,
        inDigest []byte,
        inSignerPubKey []byte,
    ) error {

        if len(inSignerPubKey) != ed25519.PublicKeySize {
            return plan.Errorf(nil, plan.BadKeyFormat, "bad ed25519 public key size")
        }
        
        if ! ed25519.Verify(inSignerPubKey, inDigest, inSig) {
            return plan.Error(nil, plan.VerifySignatureFailed, "ed25519 sig verification failed")
        }
                
        return nil
    },
}
