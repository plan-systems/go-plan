package ski


import "github.com/plan-tools/go-plan/plan"




// OpArgs maps parameter keys to values
type OpArgs struct {
    OpName              string

    // Specifies the key to be used for encrypting/decrypting/signing
    CryptoKeyID         plan.KeyID

    // A list of key IDs that are specific to a given op.
    OpKeyIDs            []plan.KeyID

    PeerPubKey          []byte

    Msg                 []byte
    Sig                 []byte

}

// OpResult is an element of any possible number of results from an SKI operation
type OpResult struct {
    info                string
    buf                 []byte
}

// OpCompletionHandler handles the result of a SKI operation
type OpCompletionHandler func(*plan.Perror, []OpResult)


// Used as inInvocation for SKI.Provider.StartSession()
const (

    InvokeNaCl = "/plan/ski/provider/nacl/1"

)



// OpArgs.OpName -- this lists all available operations for SKI.Session.DispatchOp()
// Unless otherwise stated, output from an op is returned in OpResults[0].  Ops that do NOT return anything (other than a possible error),
//    will returns no results.  Ops like OpNewIdentityRev return explicitly documented results.
const (

    // OpEncryptCommunityData encrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKeyID
    OpEncryptForCommunity   = "c_encrypt_fo"

    // OpDecryptCommunityData decrypts OpArgs.Msg using the symmetric indexed by OpArgs.CryptoKeyID
    OpDecryptFromCommunity  = "c_decrypt_from"


    // OpEncryptTo encrypts and seals OpArgs.Msg for a recipient associated with OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKeyID
    OpEncryptFor            = "encrypt_for"

    // OpDecryptFrom decrypts OpArgs.Msg from the sender's OpArgs.PeerPubKey, using the asymmetric key indexed by OpArgs.CryptoKeyID
    OpDecryptFrom           = "decrypt_from"


    // OpSignMsg creates a signature buffer for OpArgs.Msg, using the asymmetric key indexed by OpArgs.CryptoKeyID.
    // Note: len(inOpResults) == 0
    OpSignMsg               = "sign_msg"


    // OpSendCommunityKeys securely "sends" the community keys identified by OpArgs.OpKeyIDs to recipient associated with OpArgs.PeerPubKey,
    //    encrypting the resulting buffer using the asymmetric key indexed by OpArgs.CryptoKeyID.
    OpSendCommunityKeys     = "send_keys"

    // OpAcceptCommunityKeys adds the keys contained in OpArgs.Msg to its community keyring, decrypting using the key indexed by OpArgs.CryptoKeyID.
    OpAcceptCommunityKeys   = "accept_keys"

    // OpCreateCommunityKey creates a new community key and returns the associated plan.KeyID
    OpCreateCommunityKey    = "create_community_key"

    // OpNewIdentityRev issues a new personal identity revision and returns public information for that new rev.
    // Recall that the plan.KeyID for each pub key is the right-most <plan.KeyIDSz> bytes.
    // Returns:
    //     OpResults[0]: newly issued signing public key
    //     OpResults[1]: newly issued encryption public key
    OpNewIdentityRev        = "new_identity_rev"


)




// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type Provider interface {

    // StartSession starts a new session SKI.session.  In general, you should only start one session 
    StartSession(
        inInvocation        string,
        inOpsAllowed        []string,
        inOnCompletion      func(inErr *plan.Perror, inSession Session),
        inOnSessionEnded    func(inReason string),
    ) *plan.Perror

    // VerifySignature verifies that inSig is in fact the signature of inMsg signed by an owner of inSignerPubKey
    VerifySignature(inSig []byte, inMsg []byte, inSignerPubKey []byte) bool

}


  

// Session provides lambda-lifted crypto services from an opaque service provider. 
// All calls in this interface are threadsafe.
type Session interface {

    // DispatchOp implements a complete set of SKI ops
    DispatchOp(inOpArgs *OpArgs, inOnCompletion OpCompletionHandler)

    // EndSession ends this SKI session, resulting in the SKI's parent Provider to call its OnSessionClosed() callback followed by inOnCompletion.
    // Following a call to EndSession(), no more references to this session should be made -- Provider.StartSession() must be called again.
    EndSession(inReason string, inOnCompletion plan.Action)

}


var (

    // PnodeAccess is for a pnode, where it only needs to decrypt the community's PDI entry headers.
    PnodeAccess = []string{
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




// PLAN keyring names
const (

    // CommunityKeyring is the keyring all members of a given PLAN community have
    CommunityKeyring        = "/plan/keyring/community/1"

    // PersonalKeyring is one's personal keyring and is used to encrypt/decrypt private data.
    PersonalKeyring         = "/plan/keyring/personal/1"
)
