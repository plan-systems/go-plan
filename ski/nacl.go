// Package ski is a reference implementation of the SKI plugin
package ski // import "github.com/plan-tools/go-plan/ski"

import (
    "encoding/json"
    "net/http"

	plan "github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
	sign "golang.org/x/crypto/nacl/sign"
)


const (
    vouchCodecName = "/plan/ski/vouch/1"
)


// LocalSKI represents a local implementation of the SKI
type LocalSKI struct {
	keyring *keyring
}

// NewSKI initializes the SKI's keying.
func NewSKI() *LocalSKI {
	ski := &LocalSKI{keyring: newKeyring()}
	return ski
}

// ---------------------------------------------------------
//
// Top-level functions of the SKI. Because these APIs are intended to
// stand-in for the ones we'll have used across process boundaries, they
// are designed to minimize the amount of serialization and wire traffic
// required, and reduce the knowledge the SKI has about what's being done
// with these values.

// Vouch encrypts a CommunityKey for the recipient's public key and returns the encrypted buffer
func (ski *LocalSKI) Vouch(
	communityKeyID plan.KeyID,
	senderPubKey plan.IdentityPublicKey,
	recvPubKey plan.IdentityPublicKey,
) ([]byte, *plan.Perror) {
	communityKey, err := ski.keyring.GetCommunityKeyByID(communityKeyID)
	if err != nil {
		return nil, err
	}
	keyMsgBody := vouchMessage{KeyID: communityKeyID, Key: communityKey}
	serializedBody, jerr := json.Marshal(keyMsgBody)
	if jerr != nil {
		return nil, plan.Error(jerr, plan.FailedToMarshalAccessGrant, "failed to marshal access grant body content")
    }


	pdiMsgBody := &plan.PDIEntryBody{
		BodyParts: []plan.PDIBodyPart{
			plan.PDIBodyPart {
                Header:  make( http.Header ),
                Content: serializedBody,
            },
		},
    }
    pdiMsgBody.BodyParts[0].Header.Add(plan.PDIContentCodecHeaderName, vouchCodecName)

	bodyData, jerr := json.Marshal(pdiMsgBody)
	if err != nil {
		return nil, plan.Error(jerr, plan.FailedToMarshalAccessGrant, "failed to marshal access grant body header")
	}
	return ski.EncryptFor(senderPubKey, bodyData, recvPubKey)
}

    // AcceptVouch decrypts the encrypted buffer written by Vouch() and installs the wrapped community key
func (ski *LocalSKI) AcceptVouch(
    recvPubKey plan.IdentityPublicKey,
	bodyCrypt []byte,
	senderPubKey plan.IdentityPublicKey,
) *plan.Perror {

	bodyData, err := ski.DecryptFrom(recvPubKey, bodyCrypt, senderPubKey)
	if err != nil {
		return plan.Error(err, plan.FailedToDecryptAccessGrant, "failed to decrypt access grant")
    }

	pdiMsgBody := &plan.PDIEntryBody{}
	jerr := json.Unmarshal(bodyData, pdiMsgBody)
	if jerr != nil || len(pdiMsgBody.BodyParts) < 1 {
		return plan.Error(jerr, plan.FailedToProcessAccessGrant, "access grant body data failed to unmarshal")
    }
    if pdiMsgBody.BodyParts[0].Header.Get( plan.PDIContentCodecHeaderName ) != vouchCodecName {
		return plan.Errorf(nil, plan.FailedToProcessAccessGrant, "did not find valid '%s' header", plan.PDIContentCodecHeaderName)
    }
	keyMsgBody := &vouchMessage{}
	jerr = json.Unmarshal(pdiMsgBody.BodyParts[0].Content, keyMsgBody)
	if err != nil {
		return plan.Error(jerr, plan.FailedToProcessAccessGrant, "access grant content failed to unmarshal")
    }
    
	ski.keyring.InstallCommunityKey(keyMsgBody.KeyID, keyMsgBody.Key)
	return nil
}

// internal: the message sent by the Vouch process
type vouchMessage struct {
	KeyID plan.KeyID
	Key   []byte
}

// Sign accepts a message hash and returns a signature.
func (ski *LocalSKI) Sign(signer plan.IdentityPublicKey, hash []byte,
) ([]byte, *plan.Perror) {
	privateKey, err := ski.keyring.GetSigningKey(signer)
	if err != nil {
		return plan.PDIEntrySig{}, err
    }
	sig := sign.Sign(nil, hash, privateKey)
	return sig[:sign.Overhead], nil
}

// Encrypt accepts a buffer and encrypts it with the community key and returns
// the encrypted buffer (or an error). Typically the msg buffer will be a
// serialized PDIEntryBody or PDIEntryHeader. This is authenticated encryption
// but the caller will follow this call with a call to Verify the PDIEntryHash
// for validation.
func (ski *LocalSKI) Encrypt(keyID plan.KeyID, msg []byte,
) ([]byte, *plan.Perror) {
	salt := <-salts
	communityKey, err := ski.keyring.GetCommunityKeyByID(keyID)
	if err != nil {
		return nil, err
    }
    
    var ckey [32]byte
    copy(ckey[:], communityKey[:32])
    encrypted := secretbox.Seal(salt[:], msg, &salt, &ckey)
    
	return encrypted, nil
}

// EncryptFor accepts a buffer and encrypts it for the public key of the
// intended recipient and returns the encrypted buffer. Typically the msg
// buffer will be a serialized PDIEntryBody or PDIEntryHeader. Note: this
// is how the Vouch operation works under the hood except that the Vouch
// caller doesn't know what goes in the message body. Outside of Vouch
// operations, this is the basis of private messages between users. The
// caller will follow this call with a call to Sign the PDIEntryHash
func (ski *LocalSKI) EncryptFor(
	senderPubKey plan.IdentityPublicKey,
	msg []byte,
	recvPubKey plan.IdentityPublicKey,
) ([]byte, *plan.Perror) {
	salt := <-salts
	privateKey, err := ski.keyring.GetEncryptKey(senderPubKey)
	if err != nil {
		return nil, err
	}
	encrypted := box.Seal(salt[:], msg,
		&salt, recvPubKey.ToArray(), privateKey)
	return encrypted, nil
}

// Verify accepts a signature and verifies it against the public key of the
// sender. Returns the verified buffer (so it can be compared by the caller)
// and a bool indicating success.
func (ski *LocalSKI) Verify(
	pubKey plan.IdentityPublicKey,
	hash []byte,
	sig []byte,
) ([]byte, bool) {
	// TODO: this probably doesn't need to be in the SKI because it doesn't
	//       require any private key material?

	// need to re-combine the sig and hash to produce the
	// signed message that Open expects
	var signedMsg []byte
	signedMsg = append(signedMsg, sig...)
	signedMsg = append(signedMsg, hash...)
	verified, ok := sign.Open(nil, signedMsg, pubKey.ToArray())
	return verified, ok
}

// Decrypt takes an encrypted buffer and decrypts it using the community key
// and returns the cleartext buffer (or an error).
func (ski *LocalSKI) Decrypt(
	keyID plan.KeyID,
	encrypted []byte,
) ([]byte, *plan.Perror) {
	communityKey, err := ski.keyring.GetCommunityKeyByID(keyID)
	if err != nil {
		return nil, err
    }

    var ckey [32]byte
    copy(ckey[:], communityKey[:32])
    
	var salt [24]byte
	copy(salt[:], encrypted[:24])
	decrypted, ok := secretbox.Open(nil, encrypted[24:], &salt, &ckey)
	if !ok {
		return nil, plan.Error(nil, plan.FailedToDecryptCommunityData, "secretbox.Open failed to decrypt community data")
    }
    
	return decrypted, nil
}

// DecryptFrom takes an encrypted buffer and a public key, and decrypts the
// message using the recipients private key. It returns the decrypted buffer
// (or an error).
func (ski *LocalSKI) DecryptFrom(
	recvPubKey plan.IdentityPublicKey,
	encrypted []byte,
	senderPubKey plan.IdentityPublicKey,
) ([]byte, *plan.Perror) {
	privateKey, err := ski.keyring.GetEncryptKey(recvPubKey)
	if err != nil {
		return nil, err
	}
	var salt [24]byte
	copy(salt[:], encrypted[:24])
	decrypted, ok := box.Open(nil, encrypted[24:],
		&salt, senderPubKey.ToArray(), privateKey)
	if !ok {
		return nil, plan.Error(nil, plan.FailedToDecryptCommunityData, "secretbox.Open failed to decrypt")
	}
	return decrypted, nil
}

// ---------------------------------------------------------
// Key and identity management functions
// These mostly wrap the underlying keying.

// NewIdentity generates encryption and signing keys, adds them to the
// keyring, and returns the public keys associated with those private
// keys as (encryption, signing).
func (ski *LocalSKI) NewIdentity() (
	plan.IdentityPublicKey, plan.IdentityPublicKey) {
	// TODO: I don't like the return signature here. too easy to screw up
	return ski.keyring.NewIdentity()
}

// NewCommunityKey generates a new community key, adds it to the keyring,
// and returns the CommunityKeyID associated with that key.
func (ski *LocalSKI) NewCommunityKey() plan.KeyID {
	return ski.keyring.NewCommunityKey()
}
