// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"io"
	"sync"

	"github.com/plan-tools/go-plan/plan"
)

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
	AccessScopes   AccessScopes
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

	// PnodeAccess is for a pnode, where it only needs to decrypt the community's PDI entry headers.
	PnodeAccess AccessScopes = []string{
		OpDecrypt,
	}

	// GatewayROAccess is for a pgateway that only offers read-only community access (where new PDI entries CAN'T be authored)
	GatewayROAccess = append(PnodeAccess,
		OpAcceptKeys,
		OpDecryptFrom,
	)

	// GatewayRWAccess is for a pgateway that only offers normal community member access (where new PDI entries can be authored)
	GatewayRWAccess = append(GatewayROAccess,
		OpEncrypt,
		OpEncryptFor,
		OpSignMsg,
		OpSendKeys,

		OpGenerateKeys,
	)
)

/*****************************************************
** ski.Session.DispatchOp() support
**/

// OpArgs is a container for all the params needed for a SKI op to be completed.
type OpArgs struct {

	// CommunityID specifies which community key repo this Op should be dispatched to.
	CommunityID []byte

	// OpName says what SKI operation to perform and determines what inputs to use, etc. See below list of op names.
	OpName string

	// Specifies the key to be used for encrypting/decrypting/signing
	CryptoKey KeySpec

	// A list of keys that the Op does something with
	KeySpecs KeySpecs

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

	// OpSignMsg creates a signature buffer for OpArgs.Msg, using the asymmetric key indexed by OpArgs.CryptoKey.
	// Returns: len(inResults.Parts) == 0
	OpSignMsg = "sign_msg"

	/*****************************************************
	 ** Key generation & transport
	 **/

	// OpGenerateKeys generates a new key for each entry in OpArgs.KeySpecs.  Each entry in OpArgs.KeySpecs
	//     must specify a valid KeyDomain and KeyType (PubKey is ignored).  On completion, this op serializes
	//     OpArgs.KeyNames (except PubKey is now set with the new key's public key for each entry).
	// Returns:
	//     inResults.GetContentWithCodec(ski.KeySpecsProtobufCodec): newly generated key (corresponding to OpArgs.KeySpecs)
	OpGenerateKeys = "generate_keys"

	// OpSendKeys securely "sends" the keys identified by OpArgs.KeySpecs to recipient associated with OpArgs.PeerPubKey,
	//    encrypting the resulting transport buffer using the asymmetric key indexed by OpArgs.CryptoKey.
	OpSendKeys = "send_keys"

	// OpAcceptKeys adds the keys contained in OpArgs.Msg to its keyring, decrypting using the key indexed by OpArgs.CryptoKey.
	OpAcceptKeys = "accept_keys"
)

/*****************************************************
** SKI serialization codec names
**/

const (

	// KeyListProtobufCodec names the serialization codec for ski.KeyList (implemented via compilation of ski.proto)
	KeyListProtobufCodec = "/plan/ski/KeyList/1"

	// KeySpecsProtobufCodec names the serialization codec for ski.KeySpecs (implemented via compilation of ski.proto)
	KeySpecsProtobufCodec = "/plan/ski/KeySpecs/1"
)

// CryptoPkg is a generic pluggable interface that any crypto package can implement.
// It can even be partially implemented (just set nil values for funcs not implemented).
// All calls are assumed to be threadsafe.
type CryptoPkg struct {
	CryptoPkgID CryptoPkgID

	// GenerateNewKey generates a new key.
	// Pre: ioEntry.KeyInfo is already setup
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

// CryptoPkgRegistry maps a CryptoPkgID to an implementation
var cryptoPkgRegistry struct {
	sync.RWMutex
	Lookup map[CryptoPkgID]*CryptoPkg
}

// RegisterCryptoPkg registers the given provider so it can be invoked via ski.StartSession()
func RegisterCryptoPkg(
	inPkg *CryptoPkg,
) *plan.Perror {

	var err *plan.Perror
	cryptoPkgRegistry.Lock()
	pkg := cryptoPkgRegistry.Lookup[inPkg.CryptoPkgID]
	if pkg == nil {
		cryptoPkgRegistry.Lookup[inPkg.CryptoPkgID] = inPkg
	} else if pkg != inPkg {
		err = plan.Errorf(nil, plan.CryptoPkgIDAlreadyRegistered, "the CryptoPkgID %d (%s) is already registered", inPkg.CryptoPkgID, CryptoPkgID_name[int32(inPkg.CryptoPkgID)])
	}
	cryptoPkgRegistry.Unlock()

	return err
}

// GetCryptoPkg fetches the given crypto package for use
func GetCryptoPkg(
	inCryptoPkgID CryptoPkgID,
) (*CryptoPkg, *plan.Perror) {

	cryptoPkgRegistry.RLock()
	pkg := cryptoPkgRegistry.Lookup[inCryptoPkgID]
	cryptoPkgRegistry.RUnlock()

	if pkg == nil {
		return nil, plan.Errorf(nil, plan.CryptoPkgNotFound, "the CryptoPkgID %d was not found", inCryptoPkgID)
	}

	return pkg, nil
}

// VerifySignature returns nil err if the signature of inDigest plus the signer's private key matches the given signature.
// This function is threadsafe.
func VerifySignature(
	inCryptoPkgID CryptoPkgID,
	inSig []byte,
	inDigest []byte,
	inSignerPubKey []byte,
) *plan.Perror {

	pkg, err := GetCryptoPkg(inCryptoPkgID)
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
