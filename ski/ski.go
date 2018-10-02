// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"io"
	"sync"

	"github.com/plan-tools/go-plan/plan"
)

// NumKeyDomains is the number of possible ski.KeyDomain values (increment the last enum)
const NumKeyDomains = KeyDomain_PERSONAL + 1

/*****************************************************
** ski.Session
**/

// Session provides lambda-lifted crypto services from an opaque service provider.
// All calls in this interface are THREADSAFE.
type Session interface {

	// DispatchOp implements a complete set of SKI operations
	DispatchOp(inOpArgs *OpArgs, inOnCompletion OpCompletionHandler)

	//MergeKeys(inKeyList KeyList, inDst KeyPath)

	//ExportKeys(inDst KeyPath)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(inReason string, inOnCompletion plan.Action)
}

/*****************************************************
** ski.Provider
**/

// StartSessionPB is a convenience struct used for ski.Provider.StartSession()
type StartSessionPB struct {
	Invocation     plan.Block
	CommunityID    []byte
	AccessScopes   [NumKeyDomains]AccessScopes
	BaseDir        string
	OnSessionEnded func(inReason string)
}

// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type Provider interface {

	// InvocationStr returns a string that identifies this provider type
	InvocationStr() string

	// StartSession starts a new session SKI.session.  In general, you should only start one session
	StartSession(
		inPB StartSessionPB,
	) (Session, *plan.Perror)
}

/*****************************************************
** ski.InvokeProvider()
**/

// ProviderRegistry maps provider names to implementations
var providerRegistry = map[string]Provider{}

// RegisterProvider registers the given provider so it can be invoked via ski.StartSession()
func RegisterProvider(inProvider Provider) error {
	istr := inProvider.InvocationStr()
	if providerRegistry[istr] != nil {
		return plan.Errorf(nil, plan.InvocationAlreadyExists, "the ski invocation %s already exists", istr)
	}
	providerRegistry[istr] = inProvider
	return nil
}

// StartSession returns a provider implementation given an invocation block
func StartSession(
	inPB StartSessionPB,
) (Session, *plan.Perror) {

	provider := providerRegistry[inPB.Invocation.Label]
	if provider == nil || provider.InvocationStr() != inPB.Invocation.Label {
		return nil, plan.Errorf(nil, plan.InvocationNotAvailable, "ski.StartSession() failed to find provider for invocation %s", inPB.Invocation.Label)
	}

	session, err := provider.StartSession(inPB)

	return session, err
}

/*****************************************************
** ski.Provider AccessScopes
**/

// AccessScopes specifies a set of SKI ops allowed to occur in a SKI session.  A clients requests this set when starting a new session,
//     allowing the SKI provider to provision for the requested access (e.g. getting user permission of not already granted).
type AccessScopes []string

var (

	// ContentReadAccess only allows the client to decrypt data
	ContentReadAccess AccessScopes = []string{
		OpDecrypt,
		OpDecryptFrom,
	}

	// ContentAuthoringAccess allows the client to encrypt and sign data
	ContentAuthoringAccess = []string{
		OpEncrypt,
		OpEncryptFor,
        OpSign,
	}

    AddKeysAccess = []string {
		OpImportKeys,
		OpGenerateKeys,
    }
)

/*****************************************************
** ski.Session.DispatchOp() support
**/

// OpArgs is a container for all the params needed for a SKI op to be completed.
type OpArgs struct {

	// OpName says what SKI operation to perform and determines what inputs to use, etc. See below list of op names.
	OpName string

	// Key spec to used for encrypting/decrypting/signing
	CryptoKey KeyEntry

	// A list of key specs that the Op does something with
	KeySpecs KeyBundle

	// Sender/Recipient publicly available key -- a public address in the community key space
	PeerPubKey []byte

	// Input/Output buffer
	Msg []byte
}

// OpCompletionHandler handles the result of a SKI operation
type OpCompletionHandler func(inResults *plan.Block, inErr *plan.Perror)

// OpArgs.OpName -- these are the available operations for SKI.Session.DispatchOp()
// Unless otherwise stated, output from an op is returned in inResults.Content
const (

	/*****************************************************
	 ** Symmetric crypto support
	 **/

	// OpEncrypt encrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKey
	OpEncrypt = "encrypt_sym"

	// OpDecrypt decrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKey
	OpDecrypt = "decrypt_sym"

	/*****************************************************
	 ** Asymmetric crypto support
	 **/

	// OpEncryptTo encrypts and seals OpArgs.Msg for a recipient associated with OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKey
	OpEncryptFor = "encrypt_for"

	// OpDecryptFrom decrypts OpArgs.Msg from the sender's OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKey
	OpDecryptFrom = "decrypt_from"

	// OpSign creates a signature buffer for OpArgs.Msg, using the asymmetric key indexed by OpArgs.CryptoKey.
	// Returns: len(inResults.Parts) == 0
	OpSign = "sign_msg"

	/*****************************************************
	 ** Key generation & exporting
	 **/

	// OpGenerateKeys generates a new key for each entry in OpArgs.KeySpecs, returning only public info
    //     about newly generated keys (use the Export ops for getting private key data).
    // See "KeyGen mode" notes where KeyEntry is declared.  On completion, returns serialized KeyBundle
    //     where the elements correspond to OpArgs.KeySpecs (except all the key fields are set EXCEPT KeyEntry.PrivKey)
	// Returns:
	//     inResults.GetContentWithCodec(ski.KeyBundleProtobufCodec)
	OpGenerateKeys = "generate_keys"

	// OpExportNamedKeys exports the KeyEntry for each corresponding element in OpArgs.KeySpecs into a ski.KeyBundle.  
    //     This KeyBundle is then marshaled and encrypted using the asymmetric key specified by OpArgs.CryptoKey, 
    //     and than retuned in OpArgs.Msg.Content.
	// Note: if a key is not found (or is invalidly specified), this entire op will error out.
	OpExportNamedKeys = "export_named_keys"

	// OpExportKeyring operates like OpExportNamedKeys except the entire keyring specfified by OpArgs.KeySpecs.Specs[0].KeyDomain is exported.
	OpExportKeyring = "export_keyring"

	/*****************************************************
	 ** Key importing
	 **/

	// OpImportKeys adds the keys contained in OpArgs.Msg to its keyring, decrypting using the key indexed by OpArgs.CryptoKey.
	// OpArgs.Msg.Content is first decrypted using the key referenced by OpArgs.CryptoKey.  The resulting buffer unmarshalled into a
	//     ski.KeyBundle and merged with the key repo.
	// Note if a key being imported collides with an existing key (and it isn't an exact duplicate), it's spec is added to a KeyBundle
    //    that is also returned with the label ski.KeysNotImportedLabel
	OpImportKeys = "import_keys"


)

/*****************************************************
** SKI serialization codec names
**/

const (

	// KeyBundleProtobufCodec names the serialization codec for ski.KeyList (implemented via compilation of ski.proto)
	KeyBundleProtobufCodec = "/plan/ski/KeyBundle/1"

	// KeySpecsProtobufCodec names the serialization codec for ski.KeySpecs (implemented via compilation of ski.proto)
	KeySpecsProtobufCodec = "/plan/ski/KeySpecs/1"

    // KeysNotImportedLabel is the label used for a serialized KeyBundle when OpImportKeys encounters keys it couldn't import.
    KeysNotImportedLabel = "keys_not_imported"

)

// CryptoKit is a generic pluggable interface that any crypto package can implement.
// It can even be partially implemented (just set nil values for funcs not implemented).
// All calls are assumed to be threadsafe.
type CryptoKit struct {
	CryptoKitID CryptoKitID

	// Pre: ioEntry.KeyType, .KeyDomain, .CryptoKitID, and .TimeCreated is already set.
	// inRequestedKeyLen is the requested length of the private key. It can be ignored if this implmentation has a fixed key length.
	GenerateNewKey func(
		inRand io.Reader,
		inRequestedKeyLen int,
		ioEntry *KeyEntry,
	) *plan.Perror

	/*****************************************************
	** Symmetric encryption
	**/

	Encrypt func(
		inRand io.Reader,
		inMsg []byte,
		inKey []byte,
	) ([]byte, *plan.Perror)

	Decrypt func(
		inMsg []byte,
		inKey []byte,
	) ([]byte, *plan.Perror)

	/*****************************************************
	** Asymmetric encryption
	**/

	EncryptFor func(
		inRand io.Reader,
		inMsg []byte,
		inPeerPubKey []byte,
		inPrivKey []byte,
	) ([]byte, *plan.Perror)

	DecryptFrom func(
		inMsg []byte,
		inPeerPubKey []byte,
		inPrivKey []byte,
	) ([]byte, *plan.Perror)

	/*****************************************************
	** Signing & Verification
	**/

	Sign func(
		inDigest []byte,
		inSignerPrivKey []byte,
	) ([]byte, *plan.Perror)

	VerifySignature func(
		inSig []byte,
		inDigest []byte,
		inSignerPubKey []byte,
	) *plan.Perror
}

// CryptoKitRegistry maps a CryptoKitID to an implementation
var cryptoKitRegistry struct {
	sync.RWMutex
	Lookup map[CryptoKitID]*CryptoKit
}

// RegisterCryptoKit registers the given provider so it can be invoked via ski.StartSession()
func RegisterCryptoKit(
	inPkg *CryptoKit,
) *plan.Perror {

	var err *plan.Perror
	cryptoKitRegistry.Lock()
	pkg := cryptoKitRegistry.Lookup[inPkg.CryptoKitID]
	if pkg == nil {
		cryptoKitRegistry.Lookup[inPkg.CryptoKitID] = inPkg
	} else if pkg != inPkg {
		err = plan.Errorf(nil, plan.CryptoKitIDAlreadyRegistered, "the CryptoKitID %d (%s) is already registered", inPkg.CryptoKitID, CryptoKitID_name[int32(inPkg.CryptoKitID)])
	}
	cryptoKitRegistry.Unlock()

	return err
}

// GetCryptoKit fetches the given crypto package for use
func GetCryptoKit(
	inCryptoKitID CryptoKitID,
) (*CryptoKit, *plan.Perror) {

	cryptoKitRegistry.RLock()
	pkg := cryptoKitRegistry.Lookup[inCryptoKitID]
	cryptoKitRegistry.RUnlock()

	if pkg == nil {
		return nil, plan.Errorf(nil, plan.CryptoKitNotFound, "the CryptoKitID %d was not found", inCryptoKitID)
	}

	return pkg, nil
}

// VerifySignature returns nil err if the signature of inDigest plus the signer's private key matches the given signature.
// This function is threadsafe.
func VerifySignature(
	inCryptoKitID CryptoKitID,
	inSig []byte,
	inDigest []byte,
	inSignerPubKey []byte,
) *plan.Perror {

	pkg, err := GetCryptoKit(inCryptoKitID)
	if err != nil {
		return err
	}

	err = pkg.VerifySignature(
		inSig,
		inDigest,
		inSignerPubKey,
	)

	return err
}
