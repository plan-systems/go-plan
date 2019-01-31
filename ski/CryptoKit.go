
// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"io"
	"sync"

	"github.com/plan-systems/go-plan/plan"
)



// CryptoKit is a generic pluggable interface that any crypto package can implement.
// It can even be partially implemented (just set nil values for funcs not implemented).
// All calls are assumed to be threadsafe.
type CryptoKit struct {
	CryptoKitID CryptoKitID

	// Pre: ioEntry.KeyType, .KeyDomain, .CryptoKitID, and .TimeCreated is already set.
	// inRequestedKeyLen is the requested length of the private key. It is ignored 
    //     if this implementation uses fixed or implicit key lengths.
	GenerateNewKey func(
		inRand io.Reader,
		inRequestedKeyLen int,
		ioEntry *KeyEntry,
	) *plan.Err

	/*****************************************************
	** Symmetric encryption
	**/

	Encrypt func(
		inRand io.Reader,
		inMsg []byte,
		inKey []byte,
	) ([]byte, *plan.Err)

	Decrypt func(
		inMsg []byte,
		inKey []byte,
	) ([]byte, *plan.Err)

	/*****************************************************
	** Asymmetric encryption
	**/

	EncryptFor func(
		inRand io.Reader,
		inMsg []byte,
		inPeerPubKey []byte,
		inPrivKey []byte,
	) ([]byte, *plan.Err)

	DecryptFrom func(
		inMsg []byte,
		inPeerPubKey []byte,
		inPrivKey []byte,
	) ([]byte, *plan.Err)

	/*****************************************************
	** Signing & Verification
	**/

	Sign func(
		inDigest []byte,
		inSignerPrivKey []byte,
	) ([]byte, *plan.Err)

	VerifySignature func(
		inSig []byte,
		inDigest []byte,
		inSignerPubKey []byte,
	) *plan.Err
}

/*****************************************************
** CryptoKit registraton
**/

// gCryptoKitRegistry maps a CryptoKitID to an available ("registered") implementation
var gCryptoKitRegistry struct {
	sync.RWMutex
	Lookup       map[CryptoKitID]*CryptoKit
    DefaultKitID CryptoKitID
}

// RegisterCryptoKit is convenience fuction that registers the given provider so it can be invoked via ski.StartSession()
func RegisterCryptoKit(
	inPkg *CryptoKit,
) *plan.Err {

	var err *plan.Err
	gCryptoKitRegistry.Lock()
    if gCryptoKitRegistry.Lookup == nil {
        gCryptoKitRegistry.Lookup = map[CryptoKitID]*CryptoKit{} 
        gCryptoKitRegistry.DefaultKitID = inPkg.CryptoKitID
    }
	pkg := gCryptoKitRegistry.Lookup[inPkg.CryptoKitID]
	if pkg == nil {
		gCryptoKitRegistry.Lookup[inPkg.CryptoKitID] = inPkg
	} else if pkg != inPkg {
		err = plan.Errorf(nil, plan.CryptoKitIDAlreadyRegistered, "the CryptoKitID %d (%s) is already registered", inPkg.CryptoKitID, CryptoKitID_name[int32(inPkg.CryptoKitID)])
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
) (*CryptoKit, *plan.Err) {

	gCryptoKitRegistry.RLock()
    if inCryptoKitID == CryptoKitID_DEFAULT_KIT {
        inCryptoKitID = gCryptoKitRegistry.DefaultKitID
    }
	pkg := gCryptoKitRegistry.Lookup[inCryptoKitID]
	gCryptoKitRegistry.RUnlock()

	if pkg == nil {
		return nil, plan.Errorf(nil, plan.CryptoKitNotFound, "the CryptoKitID %d was not found", inCryptoKitID)
	}

	return pkg, nil
}

// VerifySignature is a convenience function that performs signature validation for any registered CryptoKit.
//  Returns nil err if the signature of inDigest plus the signer's private key matches the given signature.
// This function is threadsafe.
func VerifySignature(
	inSig          []byte,
	inDigest       []byte,
	inCryptoKitID  CryptoKitID,
	inSignerPubKey []byte,
) *plan.Err {

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


// VerifySignatureFrom is a convenience that performs signature validation for any registered CryptoKit.
func VerifySignatureFrom(
	inSig    []byte,
	inDigest []byte,
	inFrom   *PubKey,
) *plan.Err {

    if inFrom == nil {
    	return plan.Errorf(nil, plan.MissingParam, "missing 'from' param")    
    }

    // TODO: support key base conversion
    if inFrom.Encoding != 0 {
    	return plan.Errorf(nil, plan.Unimplemented, "currently only support binary keys")    
    }

    signerPubKey := inFrom.KeyBase

    err := VerifySignature(
        inSig,
        inDigest,
        inFrom.CryptoKitId,
        signerPubKey,
    )

	return err
}