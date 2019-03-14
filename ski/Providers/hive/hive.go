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

// GetSharedKeyDir returns the local dir of where keys are commonly stored
func GetSharedKeyDir() (string, error) {
    return plan.UseLocalDir("ski.hive")
}

/*
// CryptoProvider is a local implemention of ski.Provider
type CryptoProvider struct {
    ski.CryptoProvider

    sessions            []*Session
}

// NewCryptoProvider creates a new hive Provider (a ski.Provider implemented via a file in the OS)
func NewCryptoProvider() *CryptoProvider {

    var provider = &CryptoProvider{
        sessions: nil,
    }

    return provider
}

// NewSession starts a new hive SKI session locally, using a locally encrypted file.
// If len(BaseDir) == 0, then this session is heap only (and will be zeroed/lost when the session ends)
func NewSession(
	BaseDir   string,
    StoreName string,
    Passhash  []byte,
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
func (provider *CryptoProvider) InvocationStr() string {
    return providerInvocation
} 
*/

// StartSession starts a new hive SKI session locally, using a locally encrypted file.
// If len(BaseDir) == 0, then this session is heap only (and will be zeroed/lost when the session ends)
func StartSession(
	inBaseDir   string,
    inStoreName string,
    Passhash  []byte,
) (ski.Session, error) {

	session := &Session{
        nextAutoSave: time.Now(),
        BaseDir: inBaseDir,
        StoreName: inStoreName,
        keyTomeMgr: ski.NewKeyTomeMgr(),
    }

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

    return session, nil
}


// Session represents a local implementation of the SKI
type Session struct {
    ski.Session

    autoSaveMutex       sync.Mutex
    nextAutoSave        time.Time
    //fsStatus            fsStatus

    StoreName           string
	BaseDir             string

    //allowedOps          [ski.NumKeyDomains]map[string]bool

    keyTomeMgr          *ski.KeyTomeMgr       

    autoSave            *time.Ticker
}



func (session *Session) dbPathname() string {

    if len(session.BaseDir) == 0 || len(session.StoreName) == 0 {
        return ""
    }

    filename := session.StoreName + ".CryptoProvider.hive.pb"
    
    return path.Join(session.BaseDir, filename)
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
                err = nil //os.MkdirAll(path.Dir(pathname), plan.DefaultFileMode)
            } else {
                err = plan.Errorf(err, plan.KeyTomeFailedToLoad, "failed to load key tome %v", pathname)
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
                err = ioutil.WriteFile(pathname, buf, plan.DefaultFileMode)
                if err != nil {
                    err = plan.Errorf(err, plan.KeyTomeFailedToWrite, "failed to write hive")
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

    session.saveToFile()

}


// GenerateKeys -- see ski.Session
func (session *Session) GenerateKeys(srcTome *ski.KeyTome) (*ski.KeyTome, error) {

    // The ONLY reason we'd need to keep trying is natural key collisions. 
    // If this ever happened, congratulations, you've beaten the odds of a meteor hitting your house.
    tries := 5
    
    for ; tries > 0; tries-- {
        newKeys, err := srcTome.GenerateFork(crypto_rand.Reader, 32)
        if err != nil {
            return nil, err
        }

        session.keyTomeMgr.MergeTome(newKeys)
        if len(newKeys.Keyrings) == 0 {
            break
        }

        log.WithField("Keyrings", newKeys.Keyrings).Warn("generated keys failed to merge") 
    }

    if tries == 0 {
        return nil, plan.Error(nil, plan.AssertFailed, "generated keys failed to merge")
    }

    session.bumpAutoSave()
    
    return srcTome, nil
}


// FetchKeyInfo -- see ski.Session
func (session *Session) FetchKeyInfo(inKeyRef *ski.KeyRef) (*ski.KeyInfo, error) {

    opKey, err := session.keyTomeMgr.FetchKey(inKeyRef.KeyringName, nil)
    if err != nil {
        return nil, err
    }

    return opKey.KeyInfo, nil

}



// DoCryptOp -- see ski.Session
func (session *Session) DoCryptOp(opArgs *ski.CryptOpArgs) (*ski.CryptOpOut, error) {

    var err error
    opOut := &ski.CryptOpOut{}

    /*****************************************************
    ** 0) PRE-OP
    **/
    if err == nil {
        switch opArgs.CryptOp {

            case ski.CryptOp_EXPORT_TO_PEER: {
                if opArgs.TomeIn == nil {
                    err = plan.Error(nil, plan.AssertFailed, "op requires TomeIn")
                } else {
                    opArgs.BufIn, err = session.keyTomeMgr.ExportUsingGuide(opArgs.TomeIn, ski.ErrorOnKeyNotFound)
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
    if err == nil {
        if opArgs.OpKey == nil {
            err = plan.Error(nil, plan.AssertFailed, "op requires a valid KeyRef")
        } else {
            opKey, err = session.keyTomeMgr.FetchKey(opArgs.OpKey.KeyringName, opArgs.OpKey.PubKey)
            if err == nil {
                opOut.OpPubKey = opKey.KeyInfo.PubKey
                cryptoKit, err = ski.GetCryptoKit(opKey.KeyInfo.CryptoKit)
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