// Package ski is a reference implementation of the SKI plugin
package ski // import "github.com/plan-tools/go-plan/ski"

import (
    //"encoding/json"
    //"net/http"
    //"log"

	"github.com/plan-tools/go-plan/pdi"
	"github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
    sign "golang.org/x/crypto/nacl/sign"
	crypto_rand "crypto/rand"

)


const (
    keyListCodecName = "/plan/ski/keylist/1"

    // InvokeNaCl should be passed for inInvocation when calling SKI.NaclProvider.StartSession()
    InvokeNaCl = "/plan/ski/provider/nacl/1"


    // KeyEntry.KeyType
    
    // NaClSymmetricKey is a symmetric key for the SKI's NaCl implementation
    naclSymmetricKey            = 1

    // NaClEncryptionKey is a symmetric key for the SKI's NaCl implementation
    naclEncryptionKey           = 2

    // NaClSigningKey is a symmetric key for the SKI's NaCl implementation
    naclSigningKey              = 3
)



var (

    // NaclProvider is the primary "entry" point for a NaCl "provider"
    NaclProvider = &naclProvider{
    }



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



// naclProvider is a local implemention of SKI.Provider
type naclProvider struct {
    sessions            []*naclSession
}


func newNaclProvider() *naclProvider {

    var provider = &naclProvider{
        nil,
    }

    return provider
}

// NewSession initializes the SKI's keyring.
func (provider *naclProvider) NewSession() *naclSession {
	session := &naclSession{
        provider,
        newKeyring(),
        newKeyring(),
        map[string]bool{},
        nil,
    }
	return session
}


// StartSession starts a new SKI session
func (provider *naclProvider) StartSession(
    inInvocation        string,
    inOpsAllowed        []string,
    inOnCompletion      func(inErr *plan.Perror, inSession Session),
    inOnSessionEnded    func(inReason string),
) *plan.Perror {

    if inInvocation != InvokeNaCl {
        return plan.Error(nil, plan.InvocationNotAvailable, "SKI invocation not found or otherwise available")
    }

    session := provider.NewSession()
    session.onSessionEnded = inOnSessionEnded
    session.parentProvider = provider

    // Bind the request op scope
    for _, opName := range inOpsAllowed {
        session.allowedOps[opName] = true
    }

    provider.sessions = append(provider.sessions, session)

    inOnCompletion(nil, session)

    return nil
}


func (provider *naclProvider) EndSession(inSession *naclSession, inReason string) *plan.Perror {
    for i, session := range provider.sessions {
        if session == inSession {
            n := len(provider.sessions)-1
            provider.sessions[i] = provider.sessions[n]
            provider.sessions = provider.sessions[:n]
            if inSession.onSessionEnded != nil {
                inSession.onSessionEnded(inReason)
            }
            return nil
        }
    }

    return plan.Error(nil, plan.InvalidSKISession, "ski session not found")
}




// VerifySignature accepts a signature and verifies it against the public key of the signer. 
func (provider *naclProvider) VerifySignature(
	inSig []byte,
	inMsg []byte,
	inSignerPubKey []byte,
) bool {

	// need to re-combine the sig and hash to produce the
	// signed message that Open expects
	signedMsg := make([]byte, 0, len(inSig) + len(inMsg))
	signedMsg = append(signedMsg, inSig...)
    signedMsg = append(signedMsg, inMsg...)
    
    var pubKey [32]byte
    copy(pubKey[:], inSignerPubKey[:32])

    // TODO: do we need the other returned buffer for any reason?
    _, ok := sign.Open(nil, signedMsg, &pubKey)

	return ok
}




// naclSession represents a local implementation of the SKI
type naclSession struct {

    // TODO: put in mutex!?
    parentProvider      *naclProvider
    communityKeyring    *Keyring
    personalKeyring     *Keyring
    allowedOps          map[string]bool
    onSessionEnded      func(inReason string)
}



// EndSession ends this SKI session
func (session *naclSession) EndSession(inReason string, inOnCompletion plan.Action) {
    err := session.parentProvider.EndSession(session, inReason)

    inOnCompletion(err, nil)
    return
}



func (session *naclSession) DispatchOp(inArgs *OpArgs, inOnCompletion OpCompletionHandler) {

    if ! session.allowedOps[inArgs.OpName] {
        err := plan.Errorf(nil, plan.InsufficientSKIAccess, "insufficient SKI permissions for op %s", inArgs.OpName)
        inOnCompletion(err, nil)
        return
    }

    err, results := session.doOp(*inArgs)
    inOnCompletion(err, results)
}








func (session *naclSession) doOp(opArgs OpArgs) (*plan.Perror, []OpResult) {

    outResults := make([]OpResult, 0, 2)

    var err *plan.Perror

    // ====================================
    // 1) PRE-OP
    {
        switch opArgs.OpName {

            case OpSendCommunityKeys: {
                err, opArgs.Msg = session.encodeSendKeysMsg(&opArgs)
            }
        }

        if err != nil {
            return err, nil
        }
    }


    // ====================================
    // 2) LOAD TRANSPORT KEY
    var (
        peerPubKey,
        privKey32 [32]byte
        privKey64 [64]byte
    )
    privKeySz := 0
    {
        var keyBuf []byte

        switch opArgs.OpName {

            case 
            OpEncryptForCommunity,
            OpDecryptFromCommunity:
            keyBuf, err = session.communityKeyring.GetSymmetricKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            OpEncryptFor,
            OpDecryptFrom,
            OpSendCommunityKeys,
            OpAcceptCommunityKeys:
            keyBuf, err = session.personalKeyring.GetEncryptKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            OpSignMsg:
            keyBuf, err = session.personalKeyring.GetSigningKey(opArgs.CryptoKeyID)
            privKeySz = 64
        }

        if err != nil {
            return err, nil
        }
        
        if len(keyBuf) != privKeySz {
            return plan.Errorf(nil, plan.BadKeyFormat, "unexpected key length, want %d, got %s", privKeySz, len(keyBuf)), nil
        }

        copy(peerPubKey[:], opArgs.PeerPubKey)    
        switch privKeySz {
            case 32: copy(privKey32[:], keyBuf)
            case 64: copy(privKey64[:], keyBuf)
        }
    }

    // ====================================
    // 3) DO OP (fill msg, using privateKey)
    var msg []byte
    {
        switch opArgs.OpName{

            case OpSignMsg:{
                sig := sign.Sign(nil, opArgs.Msg, &privKey64)
                msg = sig[:sign.Overhead]
            }

            case OpEncryptForCommunity:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = secretbox.Seal(salt[:], opArgs.Msg, &salt, &privKey32)
            }

            case OpDecryptFromCommunity:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = secretbox.Open(nil, opArgs.Msg[24:], &salt, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptCommunityData, "secretbox.Open failed to decrypt community data")
                }
            }

            case OpEncryptFor, OpSendCommunityKeys:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = box.Seal(salt[:], opArgs.Msg, &salt, &peerPubKey, &privKey32)
            }

            case OpDecryptFrom, OpAcceptCommunityKeys:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = box.Open(nil, opArgs.Msg[24:], &salt, &peerPubKey, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptPersonalData, "secretbox.Open failed to decrypt for %s", opArgs.OpName)
                }
            }

            case OpNewIdentityRev:{
                signKey, encrKey := session.personalKeyring.NewIdentity()
                outResults = append(outResults, 
                    OpResult{info:"signingPubKey", buf:signKey},
                    OpResult{info:"encryptPubKey", buf:encrKey},
                )
            }

            case OpCreateCommunityKey:{
                keyID := session.communityKeyring.NewSymmetricKey()
                msg = keyID[:]          
            }

            default:
                err = plan.Errorf(nil, plan.UnknownSKIOpName, "unrecognized SKI operation %s", opArgs.OpName)
        }
    }

    // ====================================
    // 4) ZERO OUT PRIVATE KEY BYTES
    switch privKeySz {
        case 32: copy(privKey32[:], zero64[:32])
        case 64: copy(privKey64[:], zero64[:64])
    }



    // ====================================
    // 5) POST OP
    if err == nil {
        switch opArgs.OpName {
            case OpAcceptCommunityKeys:
                err = session.decodeAcceptKeysMsg(msg)
        }
    }

    if err == nil && msg != nil {
        outResults = append(outResults, OpResult{buf:msg})
    } 

    return err, outResults

}





func (session *naclSession) encodeSendKeysMsg(opArgs *OpArgs) (*plan.Perror, []byte){


    var keyListBuf []byte
    {
        keyList := KeyList{
            Vers: 1,
            Keys: make([]*KeyEntry, 0, len(opArgs.OpKeyIDs)),
        }

        //var perr *plan.Perror

        keysNotFound := session.communityKeyring.ExportKeys(opArgs.OpKeyIDs, &keyList)
        if len(keysNotFound) > 0 {
            return plan.Errorf(nil, plan.FailedToMarshalAccessGrant, "failed to marshal %d keys", len(keysNotFound)), nil 
        }

        var err error
        keyListBuf, err = keyList.Marshal()
        if err != nil {
            return plan.Error(err, plan.FailedToMarshalAccessGrant, "failed to marshal exported key list"), nil 
        }
    }

    body := pdi.Body {
        Parts: []*pdi.BodyPart {
            {
                ContentCodec: keyListCodecName,
                Content: keyListBuf,
            },
        },
    }

    msg, err := body.Marshal()
    if err != nil {
        return plan.Error(err, plan.FailedToMarshalAccessGrant, "failed to marshal access grant body"), nil
    }

    return nil, msg

}


func (session *naclSession) decodeAcceptKeysMsg(inMsg []byte) *plan.Perror {

    body := pdi.Body{}
    err := body.Unmarshal(inMsg)
	if err != nil {
		return plan.Error(err, plan.FailedToProcessAccessGrant, "access grant body data failed to unmarshal")
    }


    keyListBuf := body.GetPartContent(keyListCodecName)
    if keyListBuf == nil {
		return plan.Errorf(nil, plan.FailedToProcessAccessGrant, "did not find valid '%s' attachment", keyListCodecName)
    }

    keyList := KeyList{}
    
    err = keyList.Unmarshal(keyListBuf)
	if err != nil {
		return plan.Error(err, plan.FailedToProcessAccessGrant, "access grant content failed to unmarshal")
    }

    perr := session.communityKeyring.MergeKeys(keyList)
	if perr != nil {
		return perr
    }


    return nil

}





/*

// Sign accepts a message hash and returns a signature.
func (ski *naclSession) Sign(inSignKeyID plan.KeyID, hash []byte,
) ([]byte, *plan.Perror) {

	privateKey, err := ski.keyring.GetSigningKey(inSignKeyID)
	if err != nil {
		return plan.PDIEntrySig{}, err
    }
	sig := sign.Sign(nil, hash, privateKey)
	return sig[:sign.Overhead], nil
}





// Decrypt takes an encrypted buffer and decrypts it using the community key
// and returns the cleartext buffer (or an error).
func (ski *naclSession) Decrypt(
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



// ---------------------------------------------------------
// Key and identity management functions
// These mostly wrap the underlying keying.

// NewIdentity generates encryption and signing keys, adds them to the
// keyring, and returns the public keys associated with those private
// keys as (encryption, signing).
func (ski *naclSession) NewIdentity() (
	plan.IdentityPublicKey, plan.IdentityPublicKey) {
	// TODO: I don't like the return signature here. too easy to screw up
	return ski.keyring.NewIdentity()
}

// NewCommunityKey generates a new community key, adds it to the keyring,
// and returns the CommunityKeyID associated with that key.
func (ski *naclSession) NewCommunityKey() plan.KeyID {
	return ski.keyring.NewCommunityKey()
}

*/