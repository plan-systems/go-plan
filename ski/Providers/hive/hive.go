// Package hive implements ski.Provider for keys stored on the local file system (via encrypted file)
package hive

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

    // CryptoKits always available
	_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
)


const (

    // ProviderInvocation should be passed for inInvocation when calling SKI.provider.StartSession()
    providerInvocation = "/plan/ski/Provider/hive/1"
)




// Provider is a local implemention of ski.Provider
type Provider struct {
    ski.Provider

    sessions            []*Session
}

// NewProvider creates a new hive Provider (a ski.Provider implemented via a file in the OS)
func NewProvider() *Provider {

    var provider = &Provider{
        sessions: nil,
    }

    return provider
}

// NewSession initializes the SKI's keyring.
func (provider *Provider) NewSession(
    inPB ski.SessionParams,
) *Session {
	session := &Session{
        parentProvider: provider,
        Params: inPB,
        nextAutoSave: time.Now(),
        //fsStatus: 0,
        keyTomeMgr: ski.NewKeyTomeMgr(),
    }
	return session
}

    
// InvocationStr -- see interface ski.Provider
func (provider *Provider) InvocationStr() string {
    return providerInvocation
}

// StartSession starts a new SKI session
func (provider *Provider) StartSession(
    inPB ski.SessionParams,
) (ski.Session, error) {

    if inPB.Invocation.Label != provider.InvocationStr() {
        return nil, plan.Errorf(nil, plan.InvocationNotAvailable,  "ski invocation does not match (%s != %s)", inPB.Invocation.Label, provider.InvocationStr())
    }

    session := provider.NewSession(inPB)
    session.parentProvider = provider

    // Bind the request op scope -- TODO
    /*
    for i, domain := range inPB.AccessScopes {
        allowedOpsForDomain := session.allowedOps[i]
        for _, opName := range domain {
            allowedOpsForDomain[opName] = true
        }
    } */

    if err := session.loadFromFile(); err != nil {
        return nil, err
    }

    provider.sessions = append(provider.sessions, session)

    return session, nil
}



// EndSession ends to given session
func (provider *Provider) EndSession(inSession *Session, inReason string) error {
    for i, session := range provider.sessions {
        if session == inSession {
            n := len(provider.sessions)-1
            provider.sessions[i] = provider.sessions[n]
            provider.sessions = provider.sessions[:n]
            return nil
        }
    }

    return plan.Error(nil, plan.InvalidSKISession, "ski session not found")
}

// Session represents a local implementation of the SKI
type Session struct {
    ski.Session

    autoSaveMutex       sync.Mutex

    // TODO: put in mutex!?
    parentProvider      *Provider
    Params              ski.SessionParams
    nextAutoSave        time.Time
    //fsStatus            fsStatus

    //allowedOps          [ski.NumKeyDomains]map[string]bool

    keyTomeMgr          *ski.KeyTomeMgr       

    autoSave            *time.Ticker
}



func (session *Session) dbPathname() string {
    /*
    fsNameEncoding := base64.RawURLEncoding

    CS.channelDir = path.Join(
        session.Params.BaseDir, 
        "ski/Providers/fs", 
        base64.RawURLEncoding.EncodeToString(session.Params.UserID[:])*/

    if len(session.Params.BaseDir) == 0 {
        return ""
    }
    
    return path.Join(session.Params.BaseDir, "hiveKeyTome.pb")
}




func (session *Session) loadFromFile() error {

    session.autoSaveMutex.Lock()
    defer session.autoSaveMutex.Unlock()

    session.resetAutoSave()

    doClear := true
    var err error

    pathname := session.dbPathname()
    if len(pathname) > 0 {
        var buf []byte
        buf, err = ioutil.ReadFile(pathname)
        if err != nil {

            // If file doesn't exist, don't consider it an error
            if os.IsNotExist(err) {
                err = nil
            } else {
                err = plan.Errorf(err, plan.KeyTomeFailedToLoad, "Failed to load key tome file '%v'", pathname)
            }
        }

        // TODO: decrypt file buf!
        {

        }

        if err == nil && len(buf) > 0 {
            err = session.keyTomeMgr.Unmarshal(buf)
            doClear = false
        }

        // Zero out sensitive bytes
        for i := range buf {
            buf[i] = 0
        }
    }

    if doClear {
        session.keyTomeMgr.Clear()
    }

    return err
}



func (session *Session) saveToFile() error {

    session.autoSaveMutex.Lock()
    defer session.autoSaveMutex.Unlock()

    if session.autoSave != nil {
        pathname := session.dbPathname()
        if len(pathname) > 0 {

            buf, err := session.keyTomeMgr.Marshal()

            // TODO: encrypt file buf!
            if err == nil {

            }

            if err == nil {
                err = ioutil.WriteFile(pathname, buf, os.FileMode(0775))
                if err != nil {
                    err = plan.Errorf(err, plan.KeyTomeFailedToWrite, "failed to write key hive %v", pathname)
                }
            }

            if err != nil {
                log.WithError(err).Warn("saveToFile() err")
            }

            return err

        }
        session.resetAutoSave()
    }

    return nil
}


func (session *Session) resetAutoSave() {

    // When we save out successfully, stop the autosave gor outine
    {
        if session.autoSave != nil {
            session.autoSave.Stop()
            session.autoSave = nil
        }
    }

}




// EndSession -- see ski.Session
func (session *Session) EndSession(inReason string) {
   session.parentProvider.EndSession(session, inReason)

    session.saveToFile()

}

func (session *Session) checkOpParamsAndPermissions(opArgs *ski.CryptOpArgs) error {

    switch opArgs.CryptOp {

        case ski.CryptOp_GENERATE_KEYS, ski.CryptOp_EXPORT_TO_PEER:
            if opArgs.TomeIn == nil {
                return plan.Error(nil, plan.AssertFailed, "op requires TomeIn")
            }
            break;
    }
    
    return nil
}


/*
func (session *Session) checkOpParamsAndPermissions(
    inArgs *ski.OpArgs,
    ) error {

    if len(inArgs.CommunityID) < 4 {
        return plan.Errorf(nil, plan.CommunityNotSpecified, "community ID must be specified for SKI op %v", inArgs.OpName)
    }

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
    

    return nil
}
*/






// DoCryptOp -- see ski.Session
func (session *Session) DoCryptOp(opArgs *ski.CryptOpArgs) (*ski.CryptOpOut, error) {

    err := session.checkOpParamsAndPermissions(opArgs)
    if err != nil {
        return nil, err
    }

    opOut := &ski.CryptOpOut{}

    usesKey := true

    /*****************************************************
    ** 0) PRE-OP
    **/
    if err == nil {
        switch opArgs.CryptOp {

            case ski.CryptOp_EXPORT_TO_PEER: {
                opArgs.BufIn, err = session.keyTomeMgr.ExportUsingGuide(opArgs.TomeIn, ski.ErrorOnKeyNotFound)
            }

            case ski.CryptOp_GENERATE_KEYS: {
                usesKey = false
                if opArgs.TomeIn != nil {
                    newKeyTome, err := opArgs.TomeIn.GenerateFork(crypto_rand.Reader, 32)
                    if err == nil {
                        session.keyTomeMgr.MergeTome(newKeyTome)
                        session.bumpAutoSave()
                        opOut.TomeOut = opArgs.TomeIn
                        opArgs.TomeIn = nil
                    }
                }
            }
        }
    }

  
    /*****************************************************
    ** 1) LOAD OP CRYPTO KEY & KIT
    **/

    var (
        opKey *ski.KeyEntry
        cryptoKit *ski.CryptoKit
    )
    if err == nil && usesKey {
        if opArgs.OpKey == nil {
            err = plan.Error(nil, plan.AssertFailed, "op requires a valid KeyRef")
        } else {
            opKey, err := session.keyTomeMgr.FetchKey(opArgs.OpKey.KeyringName, opArgs.OpKey.PubKey)
            if err == nil {
                opOut.OpPubKey = opKey.PubKey
                cryptoKit, err = ski.GetCryptoKit(opKey.CryptoKitId)
            }
        }
    }

    /*****************************************************
    ** 2) DO OP
    **/

    if err == nil {
        switch opArgs.CryptOp {

            case ski.CryptOp_SIGN:
                opOut.BufOut, err = cryptoKit.Sign(
                    opArgs.BufIn, 
                    opKey.PrivKey)
            
            case ski.CryptOp_ENCRYPT_SYM:
                opOut.BufOut, err = cryptoKit.Encrypt(
                    crypto_rand.Reader, 
                    opArgs.BufIn, 
                    opKey.PrivKey)

            case ski.CryptOp_DECRYPT_SYM:
                opOut.BufOut, err = cryptoKit.Decrypt(
                    opArgs.BufIn, 
                    opKey.PrivKey)

            case ski.CryptOp_ENCRYPT_TO_PEER, ski.CryptOp_EXPORT_TO_PEER:
                opOut.BufOut, err = cryptoKit.EncryptFor(
                    crypto_rand.Reader, 
                    opArgs.BufIn, 
                    opArgs.PeerPubKey,
                    opKey.PrivKey)

            case ski.CryptOp_DECRYPT_FROM_PEER, ski.CryptOp_IMPORT_FROM_PEER:
                opOut.BufOut, err = cryptoKit.DecryptFrom(
                    opArgs.BufIn, 
                    opArgs.PeerPubKey,
                    opKey.PrivKey)

            case ski.CryptOp_GENERATE_KEYS:
                // no-op
                
            default:
                err = plan.Errorf(nil, plan.UnknownSKIOpName, "unrecognized SKI operation %v", opArgs.CryptOp)
        }
    }


    /*****************************************************
    ** 3) POST OP
    **/
    if err == nil {
        switch opArgs.CryptOp {

            case ski.CryptOp_EXPORT_TO_PEER:
                ski.Zero(opArgs.BufIn)

            case ski.CryptOp_IMPORT_FROM_PEER: {
                newTome := ski.KeyTome{}
                err = newTome.Unmarshal(opOut.BufOut)
                ski.Zero(opOut.BufOut)

                if err == nil {
                    session.keyTomeMgr.MergeTome(&newTome)
                    session.bumpAutoSave()
                }
                newTome.ZeroOut()
                if err != nil {
                    return nil, err
                }
            }
        }
    }

    if err != nil {
        return nil, err
    }   

    return opOut, nil

}



func (session *Session) bumpAutoSave() {

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




/*
func exportKeysIntoMsg(
    ioKeyringSet *ski.KeyringSet,
    inKeySpecs []*ski.PubKey,
) ([]byte, error) {

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
) error {

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

*/

