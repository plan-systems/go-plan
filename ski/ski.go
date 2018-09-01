package ski

import (
	"github.com/plan-tools/go-plan/plan"
)

const (

	/*****************************************************
	** PLAN keyring codec names
	**/

	// CommunityKeyring is the keyring all members of a given PLAN community have
	CommunityKeyring = "/plan/keyring/community/1"

	// PersonalKeyring is one's personal keyring and is used to encrypt/decrypt private data.
	PersonalKeyring = "/plan/keyring/personal/1"

	// StorageKeyring contains keys needed to access or commit txns on a pdi.StorageProvider
	StorageKeyring = "/plan/keyring/storage/1"

	/*****************************************************
	** PLAN SKI serialization codec names
	**/

	// KeyListProtobufCodec names the serialization codec for ski.KeyList (implemented via compilation of ski.proto)
	KeyListProtobufCodec = "/plan/ski/keylist/1"
)

/*****************************************************
** ski.Session
**/

// Session provides lambda-lifted crypto services from an opaque service provider.
// All calls in this interface are threadsafe.
type Session interface {

	// DispatchOp implements a complete set of SKI operations
	DispatchOp(inOpArgs *OpArgs, inOnCompletion OpCompletionHandler)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(inReason string, inOnCompletion plan.Action)
}

/*****************************************************
** ski.Provider
**/

// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type Provider interface {

	// StartSession starts a new session SKI.session.  In general, you should only start one session
	StartSession(
		inInvocation string,
		inAccessScopes AccessScopes,
		inOnSessionStarted func(inSession Session, inErr error),
		inOnSessionEnded func(inReason string),
	) *plan.Perror

	// VerifySignature verifies that inSig is in fact the signature of inMsg signed by an owner of inSignerPubKey
	VerifySignature(inSig []byte, inMsg []byte, inSignerPubKey []byte) *plan.Perror
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
		OpDecryptFromCommunity,
	}

	// GatewayROAccess is for a pgateway that only offers read-only community access (where new PDI entries CAN'T be authored)
	GatewayROAccess = append(PnodeAccess,
		OpAcceptCommunityKeys,
		OpDecryptFrom,
	)

	// GatewayRWAccess is for a pgateway that only offers normal community member access (where new PDI entries can be authored)
	GatewayRWAccess = append(GatewayROAccess,
		OpEncryptForCommunity,
		OpEncryptFor,
		OpSignMsg,
		OpSendCommunityKeys,

		OpCreateCommunityKey,
		OpNewIdentityRev,
	)
)

/*****************************************************
** ski.Session.DispatchOp() support
**/

// OpArgs is a container for all the params needed for a SKI op to be completed.
type OpArgs struct {

	// OpName says what SKI operatio to perform and determines what inputs to use, etc. See below list of op names.
	OpName string

	// Specifies the key to be used for encrypting/decrypting/signing
	CryptoKeyID plan.KeyID

	// A list of key IDs that are specific to a given op.
	OpKeyIDs []plan.KeyID

	// Sender/Recipient publicly available key -- a public address in the community key space
	PeerPubKey []byte

	Msg []byte
	Sig []byte
}

// OpCompletionHandler handles the result of a SKI operation
type OpCompletionHandler func(inResults *plan.Block, inErr *plan.Perror)

// Relevant labels used in the result block of OpCompletionHandler
const (

	// Used as names for returning
	PubSigningKeyName = "pub_signing_key"
	PubCryptoKeyName  = "pub_crypto_key"
)

// OpArgs.OpName -- these are the available operations for SKI.Session.DispatchOp()
// Unless otherwise stated, output from an op is returned in inResults.Content
const (

	/*****************************************************
	** Uses community keyring
	**/

	// OpEncryptCommunityData encrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKeyID
	OpEncryptForCommunity = "c_encrypt_for"

	// OpDecryptCommunityData decrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKeyID
	OpDecryptFromCommunity = "c_decrypt_from"

	// OpCreateCommunityKey creates a new community key and returns the associated plan.KeyID
	OpCreateCommunityKey = "create_community_key"

	/*****************************************************
	** Uses personal keyring
	**/

	// OpEncryptTo encrypts and seals OpArgs.Msg for a recipient associated with OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKeyID
	OpEncryptFor = "encrypt_for"

	// OpDecryptFrom decrypts OpArgs.Msg from the sender's OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKeyID
	OpDecryptFrom = "decrypt_from"

	// OpSignMsg creates a signature buffer for OpArgs.Msg, using the asymmetric key indexed by OpArgs.CryptoKeyID.
	// Returns: len(inResults.Parts) == 0
	OpSignMsg = "sign_msg"

	// OpNewIdentityRev issues a new personal identity revision and returns public information for that new rev.
	// Recall that the plan.KeyID for each pub key is the right-most <plan.KeyIDSz> bytes.
	// Returns:
	//     inResults.GetContentWithLabel(PubSigningKeyName): newly issued signing public key
	//     inResults.GetContentWithLabel(PubCryptoKeyName): newly issued encryption public key
	OpNewIdentityRev = "new_identity_rev"

	/*****************************************************
	** Uses personal AND communury keyrings
	**/

	// OpSendCommunityKeys securely "sends" the community keys identified by OpArgs.OpKeyIDs to recipient associated with OpArgs.PeerPubKey,
	//    encrypting the resulting buffer using the asymmetric key indexed by OpArgs.CryptoKeyID.
	OpSendCommunityKeys = "send_keys"

	// OpAcceptCommunityKeys adds the keys contained in OpArgs.Msg to its community keyring, decrypting using the key indexed by OpArgs.CryptoKeyID.
	OpAcceptCommunityKeys = "accept_keys"
)
