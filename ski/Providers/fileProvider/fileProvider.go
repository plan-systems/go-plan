// Package file implements ski.Provider for keys stored locally on the file system
package file

import (
    //"encoding/json"
    //"net/http"
    //"log"
    //io"
    "sync"
	crypto_rand "crypto/rand"

	"github.com/plan-tools/go-plan/ski"
	"github.com/plan-tools/go-plan/plan"
    
)


const (

    // KeysCodec is used to express the format in each KeyEntry
    KeysCodec = "/plan/ski/KeyEntry/NaCl/1"

    // ProviderInvocation should be passed for inInvocation when calling SKI.fileProvider.StartSession()
    providerInvocation = "/plan/ski/Provider/fileProvider/1"


)



var (

    // Provider is the primary "entry" point for a NaCl "provider"
    Provider = newfileProvider()



    
)




// fileProvider is a local implemention of SKI.Provider
type fileProvider struct {
    sessions            []*fileSession
}


func newfileProvider() *fileProvider {

    var provider = &fileProvider{
        nil,
    }

    return provider
}

// NewSession initializes the SKI's keyring.
func (provider *fileProvider) NewSession() *fileSession {
	session := &fileSession{
        provider,
        NewKeyring(""),
        map[string]bool{},
        nil,
    }
	return session
}


func (provider *fileProvider) InvocationStr() string {
    return providerInvocation
}


// StartSession starts a new SKI session
func (provider *fileProvider) StartSession(
    inPB ski.StartSessionPB,
) (ski.Session, *plan.Perror) {

    if inPB.Invocation.Label != provider.InvocationStr() {
        panic("ski invocation does not match")
    }

    session := provider.NewSession()
    session.onSessionEnded = inPB.OnSessionEnded
    session.parentProvider = provider

    // Bind the request op scope
    for _, opName := range inPB.AccessScopes {
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


func (provider *fileProvider) EndSession(inSession *fileSession, inReason string) *plan.Perror {
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





// fileSession represents a local implementation of the SKI
type fileSession struct {

    // TODO: put in mutex!?
    parentProvider      *fileProvider
    //KeyRepos            map[plan.CommunityID]KeyRepo
    keyring             *Keyring
    allowedOps          map[string]bool
    onSessionEnded      func(inReason string)
}






// EndSession ends this SKI session
func (session *fileSession) EndSession(inReason string, inOnCompletion plan.Action) {
    err := session.parentProvider.EndSession(session, inReason)

    inOnCompletion(nil, err)
    return
}



func (session *fileSession) DispatchOp(inArgs *ski.OpArgs, inOnCompletion ski.OpCompletionHandler) {

    if ! session.allowedOps[inArgs.OpName] {
        err := plan.Errorf(nil, plan.InsufficientSKIAccess, "insufficient SKI permissions for op %s", inArgs.OpName)
        inOnCompletion(nil, err)
        return
    }

    results, err := session.doOp(*inArgs)
    inOnCompletion(results, err)
}








func (session *fileSession) doOp(opArgs ski.OpArgs) (*plan.Block, *plan.Perror) {

    outResults := &plan.Block{}


    var err *plan.Perror

    // ====================================
    // 1) PRE-OP
    {
        switch opArgs.OpName {

            case ski.OpSendKeys: {
                opArgs.Msg, err = session.encodeSendKeysMsg(opArgs.KeySpecs)
            }
        }

        if err != nil {
            return nil, err
        }
    }


    // ====================================
    // 2) LOAD OP CRYPTO KEY
    var cryptoKey *ski.KeyEntry

    {
        var keyBuf []byte

        switch opArgs.OpName {

            case 
            ski.OpEncrypt,
            ski.OpDecrypt,
            ski.OpEncryptFor,
            ski.OpDecryptFrom,
            ski.OpSendKeys,
            ski.OpAcceptKeys,
            ski.OpSign:
            cryptoKey, err = session.KeyRepo.GetKey(opArgs.CommunityID, opArgs.CryptoKey)
        }
    }

    if err != nil {
        return nil, err
    }
    
    var cryptoPkg *ski.CryptoKit
    cryptoPkg, err = ski.GetCryptoKit(cryptoKey.CryptoKitID())
    if err != nil {
        return nil, err
    }

    // ====================================
    // 3) DO OP (fill msg, using privateKey)
    var msg []byte
    {
        switch opArgs.OpName{

            case ski.OpSign:
                msg, err = cryptoPkg.Sign(
                    opArgs.Msg, 
                    cryptoKey.PrivKey)
            

            case ski.OpEncrypt:
                msg, err = cryptoPkg.Encrypt(
                    crypto_rand.Reader, 
                    opArgs.Msg, 
                    cryptoKey.PrivKey)

            case ski.OpDecrypt:
                msg, err = cryptoPkg.Decrypt(
                    opArgs.Msg, 
                    cryptoKey.PrivKey)

            case 
            ski.OpEncryptFor, 
            ski.OpSendKeys:
                msg, err = cryptoPkg.EncryptFor(
                    crypto_rand.Reader, 
                    opArgs.Msg, 
                    opArgs.PeerPubKey,
                    cryptoKey.PrivKey)

            case 
            ski.OpDecryptFrom,
            ski.OpAcceptKeys:
                msg, err = cryptoPkg.DecryptFrom(
                    opArgs.Msg, 
                    opArgs.PeerPubKey,
                    cryptoKey.PrivKey)
    

            case ski.OpGenerateKeys:{
                err = session.keyring.GenerateKeys(opArgs.KeySpecs)
                if err == nil {
                    msg, _ = opArgs.KeySpecs.Marshal()
                    outResults.Codec = ski.KeySpecsProtobufCodec
                }
            }

            default:
                err = plan.Errorf(nil, plan.UnknownSKIOpName, "unrecognized SKI operation %s", opArgs.OpName)
        }
    }



    // ====================================
    // 4) POST OP
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





func (session *fileSession) encodeSendKeysMsg(
    inKeylist []*ski.KeySpec,
    ) ([]byte, *plan.Perror) {


    var keyListBuf []byte
    {
        keyList := ski.KeyList{
            Label: session.keyring.Label,
            KeysCodec: session.keyring.KeysCodec,
            Keys: make([]*ski.KeyEntry, 0, len(inKeylist)),
        }

        //var perr *plan.Perror

        keysNotFound := session.keyring.ExportKeys(inKeylist, &keyList)
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


func (session *fileSession) decodeAcceptKeysMsg(inMsg []byte) *plan.Perror {

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






/*

// Signing encapsulates algorithms that sign a byte digest and can verify it. 
type Signing interface {

  
}
*/