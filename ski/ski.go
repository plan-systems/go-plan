// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (
	"github.com/plan-systems/go-plan/plan"

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
	DispatchOp(inOpArgs OpArgs, inOnCompletion OpCompletionHandler)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(inReason string, inOnCompletion plan.Action)
}

/*****************************************************
** ski.Provider
**/

// SessionParams is a convenience struct used for ski.Provider.StartSession()
type SessionParams struct {
	Invocation     plan.Block
    UserID         []byte
	//AccessScopes   [NumKeyDomains]AccessScopes
	BaseDir        string
	OnSessionEnded func(inReason string)
}

// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type Provider interface {

	// InvocationStr returns a string that identifies this provider type
	InvocationStr() string

	// StartSession starts a new session SKI.session.  In general, you should only start one session
	StartSession(
		inPB SessionParams,
	) (Session, *plan.Err)
}

/*****************************************************
** ski.InvokeProvider()
**/

/*
// ProviderRegistry maps provider names to implementations
var gProviderRegistry = map[string]Provider{}

// RegisterProvider registers the given provider so it can be invoked via ski.StartSession()
func RegisterProvider(inProvider Provider) error {
	invoke := inProvider.InvocationStr()
	if gProviderRegistry[invoke] != nil {
		return plan.Errorf(nil, plan.InvocationAlreadyExists, "the ski invocation %s already exists", invoke)
	}
	gProviderRegistry[invoke] = inProvider
	return nil
}

// StartSession returns a provider implementation given an invocation block
func StartSession(
	inPB SessionParams,
) (Session, *plan.Err) {

	provider := gProviderRegistry[inPB.Invocation.Label]
	if provider == nil || provider.InvocationStr() != inPB.Invocation.Label {
		return nil, plan.Errorf(nil, plan.InvocationNotAvailable, "ski.StartSession() failed to find provider for invocation %s", inPB.Invocation.Label)
	}

	session, err := provider.StartSession(inPB)

	return session, err
}
*/

/*****************************************************
** ski.Provider AccessScopes
**/

// AccessScopes specifies a set of SKI ops allowed to occur in a SKI session.  A clients requests this set when starting a new session,
//     allowing the SKI provider to provision for the requested access (e.g. getting user permission of not already granted).
type AccessScopes []string

var (

	// ContentReadAccess only allows the client to decrypt data
	ContentReadAccess = AccessScopes{
		OpDecrypt,
		OpDecryptFrom,
	}

	// ContentAuthoringAccess allows the client to encrypt and sign data
	ContentAuthoringAccess = AccessScopes{
		OpEncrypt,
		OpEncryptFor,
        OpSign,
	}

    // AddKeysAccess allows keys to be imported and generated
    AddKeysAccess = AccessScopes{
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
	OpKeySpec PubKey

    // CommunityID specifies the community ID/scope for this op
    CommunityID []byte

	// A list of key specs that the Op does something with.  
	KeySpecs []*PubKey

	// Sender/Recipient publicly available key -- a public address in the community key space
	PeerPubKey []byte

	// Input/Output buffer
	Msg []byte
}

// OpCompletionHandler handles the result of a SKI operation
type OpCompletionHandler func(inResults *plan.Block, inErr *plan.Err)

// OpArgs.OpName -- these are the available operations for SKI.Session.DispatchOp()
// Unless otherwise stated, output from an op is returned in inResults.Content
const (

	/*****************************************************
	 ** Symmetric crypto support
	 **/

	// OpEncrypt encrypts OpArgs.Msg using the symmetric indexed by OpArgs.OpKeySpec
	OpEncrypt = "encrypt_sym"

	// OpDecrypt decrypts OpArgs.Msg using the symmetric indexed by OpArgs.OpKeySpec
	OpDecrypt = "decrypt_sym"

	/*****************************************************
	 ** Asymmetric crypto support
	 **/

	// OpEncryptTo encrypts and seals OpArgs.Msg for a recipient associated with OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.OpKeySpec
	OpEncryptFor = "encrypt_for"

	// OpDecryptFrom decrypts OpArgs.Msg from the sender's OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.OpKeySpec
	OpDecryptFrom = "decrypt_from"

	// OpSign creates a signature buffer for OpArgs.Msg, using the asymmetric key indexed by OpArgs.OpKeySpec.
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
    //     This KeyBundle is then marshalled and encrypted using the asymmetric key specified by OpArgs.OpKeySpec, 
    //     and than returned in OpArgs.Msg.Content.
	// Note: if a key is not found (or is invalidly specified), this entire op will error out.
	OpExportNamedKeys = "export_named_keys"

	// OpExportKeyring operates like OpExportNamedKeys except the entire keyring specified by OpArgs.KeySpecs.Specs[0].KeyDomain is exported.
	OpExportKeyring = "export_keyring"

	/*****************************************************
	 ** Key importing
	 **/

	// OpImportKeys adds the keys contained in OpArgs.Msg to its keyring, decrypting using the key indexed by OpArgs.OpKeySpec.
	// OpArgs.Msg.Content is first decrypted using the key referenced by OpArgs.OpKeySpec.  The resulting buffer unmarshalled into a
	//     ski.KeyBundle and merged with the key repo.
	// Note if a key being imported collides with an existing key (and it isn't an exact duplicate), it's spec is added to a KeyBundle
    //    that is also returned with the label ski.KeysNotImportedLabel
	OpImportKeys = "import_keys"


)

/*****************************************************
** SKI serialization codec names
**/

const (

	// KeyBundleProtobufCodec names the serialization codec for ski.KeyBundle (implemented via compilation of ski.proto)
	KeyBundleProtobufCodec = "/plan/ski/KeyBundle/1"

    // KeysNotImportedLabel is the label used for a serialized KeyBundle when OpImportKeys encounters keys it couldn't import.
    KeysNotImportedLabel = "keys_not_imported"

)
