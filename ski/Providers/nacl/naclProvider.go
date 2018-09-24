
package nacl

import (
    //"encoding/json"
    //"net/http"
    //"log"
	"github.com/plan-tools/go-plan/ski"
	"github.com/plan-tools/go-plan/plan"
    
	box "golang.org/x/crypto/nacl/box"
	secretbox "golang.org/x/crypto/nacl/secretbox"
    sign "golang.org/x/crypto/nacl/sign"
	crypto_rand "crypto/rand"

)


const (

    // KeysCodec is used to express the format in each KeyEntry
    KeysCodec = "/plan/ski/KeyEntry/NaCl/1"

    // ProviderInvocation should be passed for inInvocation when calling SKI.NaclProvider.StartSession()
    naclInvocation = "/plan/ski/Provider/NaCl/1"


    // KeyEntry.KeyType
    
    // NaClSymmetricKey is a symmetric key for the SKI's NaCl implementation
    naclSymmetricKey            = 1

    // NaClEncryptionKey is a symmetric key for the SKI's NaCl implementation
    naclEncryptionKey           = 2

    // NaClSigningKey is a symmetric key for the SKI's NaCl implementation
    naclSigningKey              = 3
)



var (

    // Provider is the primary "entry" point for a NaCl "provider"
    Provider = newNaclProvider()



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
        NewKeyring(ski.CommunityKeyring),
        NewKeyring(ski.PersonalKeyring),
        map[string]bool{},
        nil,
    }
	return session
}


func (provider *naclProvider) InvocationStr() string {
    return naclInvocation
}


// StartSession starts a new SKI session
func (provider *naclProvider) StartSession(
	inInvocation     plan.Block,
    inOpsAllowed     ski.AccessScopes,
    inOnSessionEnded func(inReason string),
) (ski.Session, *plan.Perror) {

    session := provider.NewSession()
    session.onSessionEnded = inOnSessionEnded
    session.parentProvider = provider

    // Bind the request op scope
    for _, opName := range inOpsAllowed {
        session.allowedOps[opName] = true
    }

    provider.sessions = append(provider.sessions, session)

    return session, nil
}

// RegisterForInvocation is how outside packages make this SKI implementation available to be invoked
func RegisterForInvocation() {
    ski.RegisterProvider(Provider)
}



/*****************************************************
** ski.InvokeProvider()
**/


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





// naclSession represents a local implementation of the SKI
type naclSession struct {

    // TODO: put in mutex!?
    parentProvider      *naclProvider
    communityKeyring    *Keyring
    personalKeyring     *Keyring
    allowedOps          map[string]bool
    onSessionEnded      func(inReason string)
}




// VerifySignature accepts a signature and verifies it against the public key of the signer. 
func (session *naclSession) VerifySignature(
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




// EndSession ends this SKI session
func (session *naclSession) EndSession(inReason string, inOnCompletion plan.Action) {
    err := session.parentProvider.EndSession(session, inReason)

    inOnCompletion(nil, err)
    return
}



func (session *naclSession) DispatchOp(inArgs *ski.OpArgs, inOnCompletion ski.OpCompletionHandler) {

    if ! session.allowedOps[inArgs.OpName] {
        err := plan.Errorf(nil, plan.InsufficientSKIAccess, "insufficient SKI permissions for op %s", inArgs.OpName)
        inOnCompletion(nil, err)
        return
    }

    results, err := session.doOp(*inArgs)
    inOnCompletion(results, err)
}








func (session *naclSession) doOp(opArgs ski.OpArgs) (*plan.Block, *plan.Perror) {

    outResults := &plan.Block{}


    var err *plan.Perror

    // ====================================
    // 1) PRE-OP
    {
        switch opArgs.OpName {

            case ski.OpSendCommunityKeys: {
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
            ski.OpEncryptForCommunity,
            ski.OpDecryptFromCommunity:
            keyBuf, err = session.communityKeyring.GetSymmetricKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            ski.OpEncryptFor,
            ski.OpDecryptFrom,
            ski.OpSendCommunityKeys,
            ski.OpAcceptCommunityKeys:
            keyBuf, err = session.personalKeyring.GetEncryptKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            ski.OpSignMsg:
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

            case ski.OpSignMsg:{
                sig := sign.Sign(nil, opArgs.Msg, &privKey64)
                msg = sig[:sign.Overhead]
            }

            case ski.OpEncryptForCommunity:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = secretbox.Seal(salt[:], opArgs.Msg, &salt, &privKey32)
            }

            case ski.OpDecryptFromCommunity:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = secretbox.Open(nil, opArgs.Msg[24:], &salt, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptCommunityData, "secretbox.Open failed to decrypt community data")
                }
            }

            case 
            ski.OpEncryptFor, 
            ski.OpSendCommunityKeys:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = box.Seal(salt[:], opArgs.Msg, &salt, &peerPubKey, &privKey32)
            }

            case 
            ski.OpDecryptFrom,
            ski.OpAcceptCommunityKeys:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = box.Open(nil, opArgs.Msg[24:], &salt, &peerPubKey, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptPersonalData, "secretbox.Open failed to decrypt for %s", opArgs.OpName)
                }
            }

            case ski.OpNewIdentityRev:{
                signKey, encrKey := session.personalKeyring.NewIdentity()
                outResults.AddContentWithLabel(signKey, ski.PubSigningKeyName)
                outResults.AddContentWithLabel(encrKey, ski.PubCryptoKeyName)
            }

            case ski. OpCreateCommunityKey:{
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
            case ski.OpAcceptCommunityKeys:
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





func (session *naclSession) encodeSendKeysMsg(opArgs *ski.OpArgs) ([]byte, *plan.Perror){


    var keyListBuf []byte
    {
        keyList := ski.KeyList{
            Label: session.communityKeyring.Label,
            KeysCodec: session.communityKeyring.KeysCodec,
            Keys: make([]*ski.KeyEntry, 0, len(opArgs.OpKeyIDs)),
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
        Codec: ski.KeyListProtobufCodec,
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


    keyListBuf := block.GetContentWithCodec(ski.KeyListProtobufCodec, 0)
    if keyListBuf == nil {
		return plan.Errorf(nil, plan.FailedToProcessAccessGrant, "did not find valid '%s' attachment", ski.KeyListProtobufCodec)
    }

    keyList := ski.KeyList{}
    
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


