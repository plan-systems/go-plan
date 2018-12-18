// Package fs implements ski.Provider for keys stored on the local file system (via encrypted file)
package fs

import (
    //"encoding/json"
    "log"
    //io"
    //"sync"
	crypto_rand "crypto/rand"

	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"
    
)


const (

    // KeysCodec is used to express the format in each KeyEntry
    KeysCodec = "/plan/ski/KeyEntry/1"

    // ProviderInvocation should be passed for inInvocation when calling SKI.fsProvider.StartSession()
    providerInvocation = "/plan/ski/Provider/fsProvider/1"


)



var (

    // Provider is the primary "entry" point
    Provider = newfsProvider()



    
)




// fsProvider is a local implemention of SKI.Provider
type fsProvider struct {
    sessions            []*fsSession
}


func newfsProvider() *fsProvider {

    var provider = &fsProvider{
        nil,
    }

    return provider
}

// NewSession initializes the SKI's keyring.
func (provider *fsProvider) NewSession(
    inPB ski.StartSessionPB,
) *fsSession {
	session := &fsSession{
        provider,
        inPB,
        &ski.KeyRepo{},
        [ski.NumKeyDomains]map[string]bool{},
        nil,
    }
	return session
}

func (provider *fsProvider) InvocationStr() string {
    return providerInvocation
}

// StartSession starts a new SKI session
func (provider *fsProvider) StartSession(
    inPB ski.StartSessionPB,
) (ski.Session, *plan.Perror) {

    if inPB.Invocation.Label != provider.InvocationStr() {
        panic("ski invocation does not match")
    }

    session := provider.NewSession(inPB)
    session.onSessionEnded = inPB.OnSessionEnded
    session.parentProvider = provider

    // Bind the request op scope
    for i, domain := range inPB.AccessScopes {
        allowedOpsForDomain := session.allowedOps[i]
        for _, opName := range domain {
            allowedOpsForDomain[opName] = true
        }
    }

    provider.sessions = append(provider.sessions, session)

    return session, nil
}

// RegisterForInvocation is how outside packages make this SKI implementation available to be invoked
func RegisterForInvocation() {
    ski.RegisterProvider(Provider)
}





func (provider *fsProvider) EndSession(inSession *fsSession, inReason string) *plan.Perror {
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





// fsSession represents a local implementation of the SKI
type fsSession struct {

    // TODO: put in mutex!?
    parentProvider      *fsProvider
    sessionPB           ski.StartSessionPB

    //KeyRepos            map[plan.CommunityID]KeyRepo
    KeyRepo             *ski.KeyRepo
    allowedOps          [ski.NumKeyDomains]map[string]bool
    onSessionEnded      func(inReason string)
}






// EndSession ends this SKI session
func (session *fsSession) EndSession(inReason string, inOnCompletion plan.Action) {
    err := session.parentProvider.EndSession(session, inReason)

    inOnCompletion(nil, err)
    return
}

func (session *fsSession) CheckPermissionsForOp(
    inArgs *ski.OpArgs,
    ) *plan.Perror {

    /*
    for i, keySpec := inKeySpecs {
        if keySpec.KeyDomain < 0 || keySpec.KeyDomain > ski.NumKeyDomains {
            return plan.Errorf(nil, plan.KeyDomainNotFound, "key domain not found {KeyDomain: %v, PubKey: %v}", keySpec.KeyDomain, keySpec.PubKey)
        }
        
        //allowedOpsForDomain := session.allowedOps[keySpec.KeyDomain]

    }
  

    if ! session.allowedOps[inArgs.OpName] {
        err := plan.Errorf(nil, plan.InsufficientSKIAccess, "insufficient SKI permissions for op %s", inArgs.OpName)
        inOnCompletion(nil, err)
        return
    }
    TODO: Implement me? 
    for i, domain := range inPB.AccessScopes {
        allowedOpsForDomain := session.allowedOps[i]
        if allowedOpsForDomain[
        for _, opName := range domain {
            allowedOpsForDomain[opName] = true
        }
    }
    */

    return nil
}

func (session *fsSession) DispatchOp(inArgs *ski.OpArgs, inOnCompletion ski.OpCompletionHandler) {

    err := session.CheckPermissionsForOp(inArgs)
    if err != nil {
        inOnCompletion(nil, err)
        return
    }

    keyringSet, err := session.KeyRepo.FetchKeyringSet(session.sessionPB.CommunityID)
    if err != nil {
        inOnCompletion(nil, err)
        return
    }

    results, err := doOp(
        keyringSet,
        *inArgs,
    )
    inOnCompletion(results, err)
}




func doOp(
    ioKeyringSet *ski.KeyringSet,
    opArgs ski.OpArgs,
) (*plan.Block, *plan.Perror) {

    outResults := &plan.Block{}

    var err *plan.Perror

    // ====================================
    // 1) PRE-OP
    {
        switch opArgs.OpName {

            case ski.OpExportNamedKeys:
                opArgs.Msg, err = exportKeysIntoMsg(ioKeyringSet, opArgs.KeySpecs.Keys)
    
        }

        if err != nil {
            return nil, err
        }
    }


    // ====================================
    // 2) LOAD OP CRYPTO KEY
    var cryptoKey *ski.KeyEntry

    {
        switch opArgs.OpName {

            case 
            ski.OpEncrypt,
            ski.OpDecrypt,
            ski.OpEncryptFor,
            ski.OpDecryptFrom,
            ski.OpSign,
            ski.OpExportNamedKeys,
            ski.OpExportKeyring,
            ski.OpImportKeys:
            cryptoKey, err = ioKeyringSet.FetchKey(&opArgs.CryptoKey)
        }

        if err != nil {
            return nil, err
        }
    }
    
    var cryptoPkg *ski.CryptoKit
    cryptoPkg, err = ski.GetCryptoKit(cryptoKey.CryptoKitId)
    if err != nil {
        return nil, err
    }

    // ====================================
    // 3) DO OP (fill msg, using privateKey)
    var msg []byte
    {
        switch opArgs.OpName{

            case 
            ski.OpSign:
                msg, err = cryptoPkg.Sign(
                    opArgs.Msg, 
                    cryptoKey.PrivKey)
            

            case 
            ski.OpEncrypt:
                msg, err = cryptoPkg.Encrypt(
                    crypto_rand.Reader, 
                    opArgs.Msg, 
                    cryptoKey.PrivKey)

            case 
            ski.OpDecrypt:
                msg, err = cryptoPkg.Decrypt(
                    opArgs.Msg, 
                    cryptoKey.PrivKey)

            case 
            ski.OpEncryptFor, 
            ski.OpExportNamedKeys,
            ski.OpExportKeyring:
                msg, err = cryptoPkg.EncryptFor(
                    crypto_rand.Reader, 
                    opArgs.Msg, 
                    opArgs.PeerPubKey,
                    cryptoKey.PrivKey)

            case 
            ski.OpDecryptFrom,
            ski.OpImportKeys:
                msg, err = cryptoPkg.DecryptFrom(
                    opArgs.Msg, 
                    opArgs.PeerPubKey,
                    cryptoKey.PrivKey)
    

            case 
            ski.OpGenerateKeys:{
                err = ioKeyringSet.GenerateNewKeys(opArgs.KeySpecs.Keys)
                if err == nil {
                    msg, _ = opArgs.KeySpecs.Marshal()
                    outResults.Codec = ski.KeyBundleProtobufCodec
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
            case ski.OpImportKeys:
                err = importKeysFromMsg(ioKeyringSet, msg)
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





func exportKeysIntoMsg(
    ioKeyringSet *ski.KeyringSet,
    inKeySpecs []*ski.KeyEntry,
) ([]byte, *plan.Perror) {

    var keysBuf []byte
    {
        // Make a KeyList that will contain a list of all the keys we're exporting
        keyBundle := ski.KeyBundle{}

        // Perform thr export
        keysNotFound := ioKeyringSet.FetchKeys(
            inKeySpecs,
            &keyBundle,
        )

        if len(keysNotFound) > 0 {
            return nil, plan.Errorf(nil, plan.FailedToMarshalKeyExport, "failed to find %d keys during export", len(keysNotFound))
        }

        var err error
        keysBuf, err = keyBundle.Marshal()
        if err != nil {
            return nil, plan.Error(err, plan.FailedToMarshalKeyExport, "failed to marshal exported keys")
        }
    }

    block := plan.Block {
        Codec: ski.KeyBundleProtobufCodec,
        Content: keysBuf,
    }

    msg, err := block.Marshal()
    if err != nil {
        return nil, plan.Error(err, plan.FailedToMarshalKeyExport, "failed to marshal exported keys")
    }

    return msg, nil

}


func importKeysFromMsg(
    ioKeyringSet *ski.KeyringSet,
    inMsg []byte,
) *plan.Perror {

    block := plan.Block{}
    err := block.Unmarshal(inMsg)
	if err != nil {
		return plan.Error(err, plan.FailedToProcessKeyImport, "key import body data failed to unmarshal")
    }

    keysBuf := block.GetContentWithCodec(ski.KeyBundleProtobufCodec, 0)
    if keysBuf == nil {
		return plan.Errorf(nil, plan.FailedToProcessKeyImport, "did not find valid '%s' attachment", ski.KeyBundleProtobufCodec)
    }

    keyBundle := ski.KeyBundle{}

    err = keyBundle.Unmarshal(keysBuf)
	if err != nil {
		return plan.Error(err, plan.FailedToProcessKeyImport, "key import content failed to unmarshal")
    }

    keysFailed := ioKeyringSet.ImportKeys(keyBundle.Keys)
    if len(keysFailed) > 0 {

        // TODO: should more be done here other than print a msg?  We don't want this to be fatal
        warn := plan.Errorf(nil, plan.KeyImportFailed, "failed to import the given keys {%v}", keysFailed)
        log.Print(warn)
    }

    return nil
}






