// Package ski is a reference implementation of the SKI plugin
package ski // import "github.com/plan-tools/go-plan/ski"

import (
    //"encoding/json"
    //"net/http"
    //"log"

	"github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
    sign "golang.org/x/crypto/nacl/sign"
	crypto_rand "crypto/rand"

)


const (

    // NaClKeysCodec is used to express the format in each KeyEntry
    NaClKeysCodec = "/plan/ski/KeyEntry/NaCl/1"

    // NaClProviderName should be passed for inInvocation when calling SKI.NaclProvider.StartSession()
    NaClProviderName = "/plan/ski/Provider/NaCl/1"


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
    NaclProvider = newNaclProvider()



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
        NewKeyring(CommunityKeyring),
        NewKeyring(PersonalKeyring),
        map[string]bool{},
        nil,
    }
	return session
}


// StartSession starts a new SKI session
func (provider *naclProvider) StartSession(
    inInvocation        string,
    inOpsAllowed        []string,
    inOnCompletion      func(inSession Session, inErr *plan.Perror),
    inOnSessionEnded    func(inReason string),
) *plan.Perror {

    if inInvocation != NaClProviderName {
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

    inOnCompletion(session, nil)

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

    inOnCompletion(nil, err)
    return
}



func (session *naclSession) DispatchOp(inArgs *OpArgs, inOnCompletion OpCompletionHandler) {

    if ! session.allowedOps[inArgs.OpName] {
        err := plan.Errorf(nil, plan.InsufficientSKIAccess, "insufficient SKI permissions for op %s", inArgs.OpName)
        inOnCompletion(nil, err)
        return
    }

    results, err := session.doOp(*inArgs)
    inOnCompletion(results, err)
}








func (session *naclSession) doOp(opArgs OpArgs) (*plan.Block, *plan.Perror) {

    outResults := &plan.Block{}


    var err *plan.Perror

    // ====================================
    // 1) PRE-OP
    {
        switch opArgs.OpName {

            case OpSendCommunityKeys: {
                opArgs.Msg, err = session.encodeSendKeysMsg(&opArgs)
            }
        }

        if err != nil {
            return nil, err
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
            return nil, err
        }
        
        if len(keyBuf) != privKeySz {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected key length, want %d, got %s", privKeySz, len(keyBuf))
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
                outResults.AddContentWithLabel(signKey, PubSigningKeyName)
                outResults.AddContentWithLabel(encrKey, PubCryptoKeyName)
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
                msg = nil
        }
    }

    if err == nil {
        if msg != nil {
            outResults.Content = msg
        }
    } else {
        outResults = nil
    }

    return outResults, err

}





func (session *naclSession) encodeSendKeysMsg(opArgs *OpArgs) ([]byte, *plan.Perror){


    var keyListBuf []byte
    {
        keyList := KeyList{
            Label: session.communityKeyring.Label,
            KeysCodec: session.communityKeyring.KeysCodec,
            Keys: make([]*KeyEntry, 0, len(opArgs.OpKeyIDs)),
        }

        //var perr *plan.Perror

        keysNotFound := session.communityKeyring.ExportKeys(opArgs.OpKeyIDs, &keyList)
        if len(keysNotFound) > 0 {
            return nil, plan.Errorf(nil, plan.FailedToMarshalAccessGrant, "failed to marshal %d keys", len(keysNotFound))
        }

        var err error
        keyListBuf, err = keyList.Marshal()
        if err != nil {
            return nil, plan.Error(err, plan.FailedToMarshalAccessGrant, "failed to marshal exported key list")
        }
    }

    block := plan.Block {
        Codec: KeyListProtobufCodec,
        Content: keyListBuf,
    }

    msg, err := block.Marshal()
    if err != nil {
        return nil, plan.Error(err, plan.FailedToMarshalAccessGrant, "failed to marshal access grant block")
    }

    return msg, nil

}


func (session *naclSession) decodeAcceptKeysMsg(inMsg []byte) *plan.Perror {

    block := plan.Block{}
    err := block.Unmarshal(inMsg)
	if err != nil {
		return plan.Error(err, plan.FailedToProcessAccessGrant, "access grant body data failed to unmarshal")
    }


    keyListBuf := block.GetContentWithCodec(KeyListProtobufCodec)
    if keyListBuf == nil {
		return plan.Errorf(nil, plan.FailedToProcessAccessGrant, "did not find valid '%s' attachment", KeyListProtobufCodec)
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


