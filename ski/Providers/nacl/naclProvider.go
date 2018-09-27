
package nacl

import (
    //"encoding/json"
    //"net/http"
    //"log"
    "io"
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
        NewKeyring(""),
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

    if inInvocation.Label != provider.InvocationStr() {
        panic("ski invocation does not match")
    }

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
    keyring             *Keyring
    allowedOps          map[string]bool
    onSessionEnded      func(inReason string)
}




// VerifySignature accepts a signature and verifies it against the public key of the signer. 
func (session *naclSession) VerifySignature(
	inSig []byte,
	inMsg []byte,
	inSignerPubKey []byte,
) *plan.Perror {

	// need to re-combine the sig and hash to produce the
	// signed message that Open expects
	signedMsg := make([]byte, 0, len(inSig) + len(inMsg))
	signedMsg = append(signedMsg, inSig...)
    signedMsg = append(signedMsg, inMsg...)
    
    var pubKey [32]byte
    copy(pubKey[:], inSignerPubKey[:32])

    // TODO: do we need the other returned buffer for any reason?
    _, ok := sign.Open(nil, signedMsg, &pubKey)

	if !ok {
        return plan.Error(nil, plan.VerifySignatureFailed, "nacl sig verification failed")
    }
            
    return nil

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

            case ski.OpSendKeys: {
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
            ski.OpEncrypt,
            ski.OpDecrypt:
            keyBuf, err = session.keyring.GetSymmetricKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            ski.OpEncryptFor,
            ski.OpDecryptFrom,
            ski.OpSendKeys,
            ski.OpAcceptKeys:
            keyBuf, err = session.keyring.GetEncryptKey(opArgs.CryptoKeyID)
            privKeySz = 32

            case
            ski.OpSignMsg:
            keyBuf, err = session.keyring.GetSigningKey(opArgs.CryptoKeyID)
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

            case ski.OpEncrypt:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = secretbox.Seal(salt[:], opArgs.Msg, &salt, &privKey32)
            }

            case ski.OpDecrypt:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = secretbox.Open(nil, opArgs.Msg[24:], &salt, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt data")
                }
            }

            case 
            ski.OpEncryptFor, 
            ski.OpSendKeys:{
                var salt [24]byte
                crypto_rand.Read(salt[:])
                msg = box.Seal(salt[:], opArgs.Msg, &salt, &peerPubKey, &privKey32)
            }

            case 
            ski.OpDecryptFrom,
            ski.OpAcceptKeys:{
                var salt [24]byte
                copy(salt[:], opArgs.Msg[:24])
                
                var ok bool
                msg, ok = box.Open(nil, opArgs.Msg[24:], &salt, &peerPubKey, &privKey32)
                if ! ok {
                    err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt for %s", opArgs.OpName)
                }
            }

            case ski.OpNewIdentityRev:{
                signKey, encrKey := session.keyring.NewIdentity()
                outResults.AddContentWithLabel(signKey, ski.PubSigningKeyName)
                outResults.AddContentWithLabel(encrKey, ski.PubCryptoKeyName)
            }

            case ski. OpCreateSymmetricKey:{
                keyID := session.keyring.NewSymmetricKey()
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
            case ski.OpAcceptKeys:
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
            Label: session.keyring.Label,
            KeysCodec: session.keyring.KeysCodec,
            Keys: make([]*ski.KeyEntry, 0, len(opArgs.OpKeyIDs)),
        }

        //var perr *plan.Perror

        keysNotFound := session.keyring.ExportKeys(opArgs.OpKeyIDs, &keyList)
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

    perr := session.keyring.MergeKeys(keyList)
	if perr != nil {
		return perr
    }


    return nil

}



// CryptoPkg is used with ski.RegisterCryptoPkg() so it is freely available.
var CryptoPkg = ski.CryptoPkg{

    CryptoPkgID: ski.CryptoPkgID_NACL,
    CryptoPkgName: "NaCl",

	/*****************************************************
	** Symmetric encryption
	**/

    Encrypt: func(
        inRand io.Reader, 
        inMsg []byte,
        inKey []byte,
    ) ([]byte, error) {

        if len(inKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected key length, want %d, got %s", 32, len(inKey))
        }

        var salt [24]byte
        inRand.Read(salt[:])

        var privKey [32]byte
        copy(privKey[:], inKey[:32])
        
        msg := secretbox.Seal(salt[:], inMsg, &salt, &privKey)

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])

        return msg, nil
    },


    Decrypt: func(
        inMsg []byte,
        inKey []byte,
    ) ([]byte, error) {

        var salt [24]byte
        copy(salt[:], inMsg[:24])
        
        var privKey [32]byte
        copy(privKey[:], inKey[:32])

        var err *plan.Perror
        msg, ok := secretbox.Open(nil, inMsg[24:], &salt, &privKey)
        if ! ok {
            err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt data")
        }

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])


        return msg, err
    },

	/*****************************************************
	** Asymmetric encryption
	**/

    EncryptFor: func(
        inRand io.Reader, 
        inMsg []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {

        if len(inPeerPubKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected peer pub key length, want %d, got %s", 32, len(inPeerPubKey))
        }

        if len(inPrivKey) != 32 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected private key length, want %d, got %s", 32, len(inPrivKey))
        }

        var salt [24]byte
        inRand.Read(salt[:])

        var privKey, peerPubKey [32]byte
        copy(peerPubKey[:], inPeerPubKey[:32])
        copy(privKey[:], inPrivKey[:32])

        msg := box.Seal(salt[:], inMsg, &salt, &peerPubKey, &privKey)

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])

        return msg, nil
    },


    DecryptFrom: func(
        inMsg []byte,
        inKey []byte,
        inPeerPubKey []byte,
        inPrivKey []byte,
    ) ([]byte, error) {

        var salt [24]byte
        copy(salt[:], inMsg[:24])
        
        var privKey, peerPubKey [32]byte
        copy(peerPubKey[:], inPeerPubKey[:32])
        copy(privKey[:], inPrivKey[:32])

        var err *plan.Perror
        msg, ok := box.Open(nil, inMsg[24:], &salt, &peerPubKey, &privKey)
        if ! ok {
            err = plan.Errorf(nil, plan.FailedToDecryptData, "secretbox.Open failed to decrypt for peer %v", inPeerPubKey)
        }

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:32])

        return msg, err
    },

	/*****************************************************
	** Signing & Verification
	**/

    Sign: func(
        inDigest []byte,
        inSignerPrivKey []byte,
    ) ([]byte, error) {

        if len(inSignerPrivKey) != 64 {
            return nil, plan.Errorf(nil, plan.BadKeyFormat, "unexpected key length, want %d, got %s", 64, len(inSignerPrivKey))
        }

        var privKey [64]byte
        copy(privKey[:], inSignerPrivKey[:64])

        sig := sign.Sign(nil, inDigest, &privKey)

        // Don't leave any private key bytes in memory
        copy(privKey[:], zero64[:64])

        return sig[:sign.Overhead], nil
    },


    VerifySignature: func(
        inSig []byte,
        inDigest []byte,
        inSignerPubKey []byte,
    ) error {

        // need to re-combine the sig and hash to produce the
        // signed message that Open expects
        signedMsg := make([]byte, 0, len(inSig) + len(inDigest))
        signedMsg = append(signedMsg, inSig...)
        signedMsg = append(signedMsg, inDigest...)
        
        var pubKey [32]byte
        copy(pubKey[:], inSignerPubKey[:32])

        // TODO: do we need the other returned buffer for any reason?
        _, ok := sign.Open(nil, signedMsg, &pubKey)

        if !ok {
            return plan.Error(nil, plan.VerifySignatureFailed, "nacl sig verification failed")
        }
                
        return nil
        
    },

}


 




/*

// Signing encapsulates algorithms that sign a byte digest and can verify it. 
type Signing interface {

  
}
*/