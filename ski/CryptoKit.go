// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"io"
	"sync"
)

// CryptoKit is a generic pluggable interface that any crypto package can implement.
// It can even be partially implemented (just set nil values for funcs not implemented).
// All calls are assumed to be threadsafe.
type CryptoKit interface {

	// CryptoKitID univeserally identifies a specific crypto suite and version.
	CryptoKitID() CryptoKitID

	// Pre: ioEntry.KeyType, .KeyDomain, .CryptoKitID, and .TimeCreated are set.
	// inRequestedKeySz is the requested length of the private key (ignored for some implementations)
	GenerateNewKey(
		inRequestedKeySz int,
		ioRand           io.Reader,
		ioEntry          *KeyEntry,
	) error

	/*****************************************************
	** Symmetric encryption (via arbitrary password)
	**/

	// Encrypts a buffer using any arbitrary-length password
	EncryptUsingPassword(
		ioRand io.Reader,
		inMsg  []byte,
		inPwd  []byte,
	) ([]byte, error)

	// Decrypt a buffer encrypted via EncryptUsingPassword()
	DecryptUsingPassword(
		inMsg []byte,
		inPwd []byte,
	) ([]byte, error)

	/*****************************************************
	** Symmetric encryption
	**/

	Encrypt(
		ioRand io.Reader,
		inMsg  []byte,
		inKey  []byte,
	) ([]byte, error)

	Decrypt(
		inMsg []byte,
		inKey []byte,
	) ([]byte, error)

	/*****************************************************
	** Asymmetric encryption
	**/

	EncryptFor(
		ioRand       io.Reader,
		inMsg        []byte,
		inPeerPubKey []byte,
		inPrivKey    []byte,
	) ([]byte, error)

	DecryptFrom(
		inMsg        []byte,
		inPeerPubKey []byte,
		inPrivKey    []byte,
	) ([]byte, error)

	/*****************************************************
	** Signing & Verification
	**/

	Sign(
		inDigest        []byte,
		inSignerPrivKey []byte,
	) ([]byte, error)

	VerifySignature(
		inSig          []byte,
		inDigest       []byte,
		inSignerPubKey []byte,
	) error
}

/*****************************************************
** CryptoKit registraton
**/

// gCryptoKitRegistry maps a CryptoKitID to an available ("registered") implementation
var gCryptoKitRegistry struct {
	sync.RWMutex
	Lookup map[CryptoKitID]CryptoKit
}

// RegisterCryptoKit is convenience fuction that registers the given provider so it can be invoked via ski.StartSession()
func RegisterCryptoKit(
	inKit CryptoKit,
) error {
	var err error
	gCryptoKitRegistry.Lock()
	if gCryptoKitRegistry.Lookup == nil {
		gCryptoKitRegistry.Lookup = map[CryptoKitID]CryptoKit{}
	}
	kitID := inKit.CryptoKitID()
	existing := gCryptoKitRegistry.Lookup[kitID]
	if existing == nil {
		gCryptoKitRegistry.Lookup[kitID] = inKit
	} else if existing != inKit {
		err = ErrCode_CryptoKitAlreadyRegistered.ErrWithMsgf("the CryptoKit %d (%s) is already registered", kitID, kitID.String())
	}
	gCryptoKitRegistry.Unlock()

	return err
}

/*****************************************************
** CryptoKit convenience functions
**/

// GetCryptoKit fetches a given registered crypto module for use
// If the associated CryptoKit has not been registered, an error is returned.
func GetCryptoKit(
	inCryptoKitID CryptoKitID,
) (CryptoKit, error) {

	gCryptoKitRegistry.RLock()
	kit := gCryptoKitRegistry.Lookup[inCryptoKitID]
	gCryptoKitRegistry.RUnlock()

	if kit == nil {
		return nil, ErrCode_CryptoKitAlreadyRegistered.ErrWithMsgf("CryptoKit %d not found", inCryptoKitID)
	}

	return kit, nil
}

// VerifySignature is a convenience function that performs signature validation for any registered CryptoKit.
// Returns nil err if the signature of inDigest plus the signer's private key matches the given signature.
// This function is threadsafe.
func VerifySignature(
	inCryptoKitID  CryptoKitID,
	inSig          []byte,
	inDigest       []byte,
	inSignerPubKey []byte,
) error {

	kit, err := GetCryptoKit(inCryptoKitID)
	if err != nil {
		return err
	}

	err = kit.VerifySignature(
		inSig,
		inDigest,
		inSignerPubKey,
	)
	return err
}
