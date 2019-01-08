// Package filesys implements ski.Provider for keys stored on the local file system (via encrypted file)
package filesys

import (
    //"encoding/json"
    log "github.com/sirupsen/logrus"
    "path"
    "io/ioutil"
    "os"
    "time"
    //io"
    "sync"
	crypto_rand "crypto/rand"

	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"
    
)


const (

    // ProviderInvocation should be passed for inInvocation when calling SKI.fileRepo.StartSession()
    providerInvocation = "/plan/ski/Provider/filesys/1"
)



var (

    // Provider is the primary "entry" point
    Provider = newfileRepo()
)


func init() {
    ski.RegisterProvider(Provider)
}




// fileRepo is a local implemention of SKI.Provider
type fileRepo struct {
    sessions            []*fsSession
}


func newfileRepo() *fileRepo {

    var provider = &fileRepo{
        nil,
    }

    return provider
}

// NewSession initializes the SKI's keyring.
func (provider *fileRepo) NewSession(
    inPB ski.SessionParams,
) *fsSession {
	session := &fsSession{
        parentProvider: provider,
        Params: inPB,
        nextAutoSave: time.Now(),
        //fsStatus: 0,
        KeyRepo: ski.NewKeyRepo(),
        allowedOps: [ski.NumKeyDomains]map[string]bool{},
    }
	return session
}

    

func (provider *fileRepo) InvocationStr() string {
    return providerInvocation
}

// StartSession starts a new SKI session
func (provider *fileRepo) StartSession(
    inPB ski.SessionParams,
) (ski.Session, *plan.Perror) {

    if inPB.Invocation.Label != provider.InvocationStr() {
        return nil, plan.Errorf(nil, plan.InvocationNotAvailable,  "ski invocation does not match (%s != %s)", inPB.Invocation.Label, provider.InvocationStr())
    }

    session := provider.NewSession(inPB)
    session.onSessionEnded = inPB.OnSessionEnded
    session.parentProvider = provider

    // Bind the request op scope -- TODO
    /*
    for i, domain := range inPB.AccessScopes {
        allowedOpsForDomain := session.allowedOps[i]
        for _, opName := range domain {
            allowedOpsForDomain[opName] = true
        }
    } */

    err := session.loadFromFile()
    if err != nil {
        return nil, err
    }

    provider.sessions = append(provider.sessions, session)

    return session, nil
}





func (provider *fileRepo) EndSession(inSession *fsSession, inReason string) *plan.Perror {
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

/*
type fsStatus int32
const (
	LoadedFromStore        fsStatus = 1 << iota
)
*/

// fsSession represents a local implementation of the SKI
type fsSession struct {
    autoSaveMutex       sync.Mutex

    // TODO: put in mutex!?
    parentProvider      *fileRepo
    Params              ski.SessionParams
    nextAutoSave        time.Time
    //fsStatus            fsStatus

    //KeyRepos            map[plan.CommunityID]KeyRepo
    KeyRepo             *ski.KeyRepo
    allowedOps          [ski.NumKeyDomains]map[string]bool
    onSessionEnded      func(inReason string)

    autoSave            *time.Ticker
}



func (session *fsSession) dbPathname() string {
    /*
    fsNameEncoding := base64.RawURLEncoding

    CS.channelDir = path.Join(
        session.Params.BaseDir, 
        "ski/Providers/fs", 
        base64.RawURLEncoding.EncodeToString(session.Params.UserID[:])*/

    return path.Join(session.Params.BaseDir, "filesysKeyTome.pb")
}




func (session *fsSession) loadFromFile() *plan.Perror {

    session.autoSaveMutex.Lock()
    defer session.autoSaveMutex.Unlock()

    session.resetAutoSave()

    session.KeyRepo.Clear()

    pathname := session.dbPathname()
    buf, ferr := ioutil.ReadFile(pathname)
    if ferr != nil {

        // If file doesn't exist, don't consider it an error
        if os.IsNotExist(ferr) {
            return nil
        }
        return plan.Errorf(ferr, plan.KeyTomeFailedToLoad, "Failed to load key tome file '%v'", pathname)
    }

    var err *plan.Perror

    // TODO: decrypt file buf!
    {

    }

    if err == nil {
        err = session.KeyRepo.Unmarshal(buf)
    }

    // Zero out sensitive bytes
    for i := range buf {
        buf[i] = 0
    }

    return err
}



func (session *fsSession) saveToFile() *plan.Perror {

    session.autoSaveMutex.Lock()
    defer session.autoSaveMutex.Unlock()

    if session.autoSave != nil {
  
        buf, err := session.KeyRepo.Marshal()
        if err != nil {
            return err
        }

        // TODO: encrypt file buf!
        {

        }

        ferr := ioutil.WriteFile(session.dbPathname(), buf, os.FileMode(0775))
        if ferr != nil {
        return plan.Errorf(ferr, plan.KeyTomeFailedToWrite, "Failed to write key tome file")
        }

        session.resetAutoSave()
    }

    return nil
}


func (session *fsSession) resetAutoSave() {

    // When we save out successfully, stop the autosave gor outine
    {
        if session.autoSave != nil {
            session.autoSave.Stop()
            session.autoSave = nil
        }
    }

}




// EndSession ends this SKI session
func (session *fsSession) EndSession(inReason string, inOnCompletion plan.Action) {
    err := session.parentProvider.EndSession(session, inReason)

    session.saveToFile()

    inOnCompletion(nil, err)
    return
}

func (session *fsSession) CheckOpParamsAndPermissions(
    inArgs *ski.OpArgs,
    ) *plan.Perror {

    if len(inArgs.CommunityID) < 4 {
        return plan.Errorf(nil, plan.CommunityNotSpecified, "community ID must be specified for SKI op %v", inArgs.OpName)
    }

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

    err := session.CheckOpParamsAndPermissions(inArgs)
    if err != nil {
        inOnCompletion(nil, err)
        return
    }

    mutates := false
    switch inArgs.OpName {
        case 
        ski.OpGenerateKeys,
        ski.OpImportKeys:
            mutates = true
    }

    keyringSet, err := session.KeyRepo.FetchKeyringSet(inArgs.CommunityID, mutates)
    if err != nil {
        inOnCompletion(nil, err)
        return
    }

    plan.Assert(keyringSet != nil, "expected keyringSet for non-nil error!")

    logE := log.WithFields(log.Fields{ 
        "desc": "ski.Provider.filesys.DispatchOp()",
        "OpName": inArgs.OpName,
    })
    logE.Trace( "doOp()" )

    results, err := doOp(
        keyringSet,
        *inArgs,
    )
 
    if err != nil {
        logE := logE.WithError(err)
        logE.Info("doOp() returned ERROR")
    }

    if mutates {
        session.autoSaveMutex.Lock()
        session.nextAutoSave = time.Now().Add(1600 * time.Millisecond)
        if session.autoSave == nil {
            session.autoSave = time.NewTicker(500 * time.Millisecond)
            go func() {
                for t := range session.autoSave.C {
                    session.autoSaveMutex.Lock()
                    saveNow := t.After(session.nextAutoSave)
                    session.autoSaveMutex.Unlock()
                    if saveNow {
                        session.saveToFile()
                        break
                    }
                }
            }()
        }
        session.autoSaveMutex.Unlock()
    }

    inOnCompletion(results, err)
}




func doOp(
    ioKeyringSet *ski.KeyringSet,
    opArgs ski.OpArgs,
) (*plan.Block, *plan.Perror) {

    outResults := &plan.Block{}

    var err *plan.Perror

    /*****************************************************
    ** 1) LOAD OP CRYPTO KEY & KIT
    **/
    var cryptoKey *ski.KeyEntry
    var cryptoPkg *ski.CryptoKit

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
            cryptoKey, err = ioKeyringSet.FetchKey(&opArgs.OpKeySpec)
        }

        if err != nil {
            return nil, err
        }

        if cryptoKey != nil {
            cryptoPkg, err = ski.GetCryptoKit(cryptoKey.CryptoKitId)
        }
    
        if err != nil {
            return nil, err
        }
    }


    /*****************************************************
    ** 2) PRE-OP
    **/
    {
        switch opArgs.OpName {

            case ski.OpExportNamedKeys:
                opArgs.Msg, err = exportKeysIntoMsg(ioKeyringSet, opArgs.KeySpecs)
    
        }

        if err != nil {
            return nil, err
        }
    }


    /*****************************************************
    ** 3) DO OP
    **/
    var msg []byte
    {
        switch opArgs.OpName {

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
                kb := ski.KeyBundle{}
                kb.Keys, err = ioKeyringSet.GenerateNewKeys(opArgs.KeySpecs)
                if err == nil {
                    msg, _ = kb.Marshal()
                    outResults.Codec = ski.KeyBundleProtobufCodec
                }
            }

            default:
                err = plan.Errorf(nil, plan.UnknownSKIOpName, "unrecognized SKI operation %s", opArgs.OpName)
        }
    }



    /*****************************************************
    ** 4) POST OP
    **/
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
    inKeySpecs []*ski.PubKey,
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
        log.WithFields(log.Fields{
            "code": plan.KeyImportFailed,
            "key_failed": keysFailed,
        }).Warn("failed to import the given keys")
    }

    return nil
}






