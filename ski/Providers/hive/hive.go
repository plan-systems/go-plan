// Package hive implements ski.Provider for keys stored on the local file system (via encrypted file)
package hive

import (
	//"encoding/json"
	"path"
	"io/ioutil"
	"os"
	"time"
	//io"
	"sync"
	crypto_rand "crypto/rand"

	"github.com/plan-systems/go-ptools"
	"github.com/plan-systems/go-plan/ski"
	"github.com/plan-systems/go-plan/plan"

	// CryptoKits always available
	_ "github.com/plan-systems/go-plan/ski/CryptoKits/nacl"
	_ "github.com/plan-systems/go-plan/ski/CryptoKits/ed25519"
)


const (

	// ProviderInvocation should be passed for inInvocation when calling SKI.provider.StartSession()
	providerInvocation = "/plan/ski/Provider/hive/1"
)

// StartSession starts a new hive SKI session locally, using a locally encrypted file.
// If len(BaseDir) == 0, then this session is heap only (and will be zeroed/lost when the session ends)
func StartSession(
	inBaseDir   string,
	inStoreName string,
	inPass		[]byte,
) (ski.Session, error) {

	sess := &Session{
		nextAutoSave: time.Now(),
		BaseDir: inBaseDir,
		StoreName: inStoreName,
		keyTomeMgr: ski.NewKeyTomeMgr(),
		defaultCryptoKit: ski.CryptoKitID_NaCl,
		hivePass: make([]byte, len(inPass)),
	}

	copy(sess.hivePass, inPass)

	sess.SetLogLabel("ski.hive") 

	if err := sess.loadFromFile(); err != nil {
		return nil, err
	}

	return sess, nil
}


// Session represents a local implementation of the SKI
type Session struct {
	ski.Session
	ptools.Logger

	defaultCryptoKit	ski.CryptoKitID
	autoSaveMutex	   sync.Mutex
	nextAutoSave		time.Time
	StoreName		   string
	BaseDir			 string
	keyTomeMgr		  *ski.KeyTomeMgr	   
	autoSave			*time.Ticker
	hivePass			[]byte
}



func (sess *Session) dbPathname() string {

	if len(sess.BaseDir) == 0 || len(sess.StoreName) == 0 {
		return ""
	}
	
	return path.Join(sess.BaseDir, sess.StoreName)
}


func (sess *Session) loadFromFile() error {

	sess.autoSaveMutex.Lock()
	defer sess.autoSaveMutex.Unlock()

	sess.resetAutoSave()

	doClear := true
	var err error

	pathname := sess.dbPathname()
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

		if err == nil {
			var kit *ski.CryptoKit
			kit, err = ski.GetCryptoKit(sess.defaultCryptoKit)
			buf, err = kit.DecryptUsingPassword(buf, sess.hivePass)
		}

		if err == nil && len(buf) > 0 {
			err = sess.keyTomeMgr.Unmarshal(buf)
			doClear = false
		}

		ski.Zero(buf)
	}

	if doClear {
		sess.keyTomeMgr.Clear()
	}

	return err
}



func (sess *Session) saveToFile() error {

	sess.autoSaveMutex.Lock()
	defer sess.autoSaveMutex.Unlock()

	if sess.autoSave != nil {
		pathname := sess.dbPathname()
		if len(pathname) > 0 {

			buf, err := sess.keyTomeMgr.Marshal()

			if err == nil {
				var kit *ski.CryptoKit
				kit, err = ski.GetCryptoKit(sess.defaultCryptoKit)
				buf, err = kit.EncryptUsingPassword(crypto_rand.Reader, buf, sess.hivePass)
			}

			if err == nil {
				err = ioutil.WriteFile(pathname, buf, plan.DefaultFileMode)
				if err != nil {
					err = plan.Errorf(err, plan.KeyTomeFailedToWrite, "failed to write hive")
				}
			}

			if err != nil {
				sess.Error("couldn't save to file: ", err)
			}

			return err
		}
		sess.resetAutoSave()
	}

	return nil
}


func (sess *Session) resetAutoSave() {

	// When we save out successfully, stop the autosave gor outine
	{
		if sess.autoSave != nil {
			sess.autoSave.Stop()
			sess.autoSave = nil
		}
	}
}




// EndSession -- see ski.Session
func (sess *Session) EndSession(inReason string) {

	sess.saveToFile()

	ski.Zero(sess.hivePass)
}


// GenerateKeys -- see ski.Session
func (sess *Session) GenerateKeys(srcTome *ski.KeyTome) (*ski.KeyTome, error) {

	// The ONLY reason we'd need to keep trying is natural key collisions. 
	// If this ever happened, congratulations, you've beaten the odds of a meteor hitting your house millions of times in a row.
	tries := 3
	
	for ; tries > 0; tries-- {
		newKeys, err := srcTome.GenerateFork(crypto_rand.Reader, 32)
		if err != nil {
			return nil, err
		}

		sess.keyTomeMgr.MergeTome(newKeys)
		if failCount := len(newKeys.Keyrings); failCount > 0 {
    		sess.Warnf("%d generated keys failed to merge", failCount) 
		} else {
            break
        }
	}

	if tries == 0 {
		return nil, plan.Error(nil, plan.AssertFailed, "generated keys failed to merge")
	}

	sess.bumpAutoSave()
	
	return srcTome, nil
}


// FetchKeyInfo -- see ski.Session
func (sess *Session) FetchKeyInfo(inKeyRef *ski.KeyRef) (*ski.KeyInfo, error) {

	opKey, err := sess.keyTomeMgr.FetchKey(inKeyRef.KeyringName, nil)
	if err != nil {
		return nil, err
	}

	return opKey.KeyInfo, nil

}



// DoCryptOp -- see ski.Session
func (sess *Session) DoCryptOp(opArgs *ski.CryptOpArgs) (*ski.CryptOpOut, error) {

	var err error
	opOut := &ski.CryptOpOut{}

	/*****************************************************
	** 0) PRE-OP
	**/
	if err == nil {
		switch opArgs.CryptOp {

			case ski.CryptOp_EXPORT_TO_PEER, ski.CryptOp_EXPORT_USING_PW: {
				if opArgs.TomeIn == nil {
					err = plan.Error(nil, plan.AssertFailed, "op requires TomeIn")
				} else {
					opArgs.BufIn, err = sess.keyTomeMgr.ExportUsingGuide(opArgs.TomeIn, ski.ErrorOnKeyNotFound)
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
		switch opArgs.CryptOp {
			case ski.CryptOp_EXPORT_USING_PW, 
				 ski.CryptOp_IMPORT_USING_PW:
				cryptoKit, err = ski.GetCryptoKit(opArgs.DefaultCryptoKit)

			default:
				if opArgs.OpKey == nil {
					err = plan.Error(nil, plan.AssertFailed, "op requires a valid KeyRef")
				} else {
					opKey, err = sess.keyTomeMgr.FetchKey(opArgs.OpKey.KeyringName, opArgs.OpKey.PubKey)
					if err == nil {
						opOut.OpPubKey = opKey.KeyInfo.PubKey
						cryptoKit, err = ski.GetCryptoKit(opKey.KeyInfo.CryptoKit)
					}
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

			case ski.CryptOp_ENCRYPT_TO_PEER, 
				 ski.CryptOp_EXPORT_TO_PEER:
				opOut.BufOut, err = cryptoKit.EncryptFor(
					crypto_rand.Reader, 
					opArgs.BufIn, 
					opArgs.PeerKey,
					opKey.PrivKey)

			case ski.CryptOp_DECRYPT_FROM_PEER,
				 ski.CryptOp_IMPORT_FROM_PEER:
				opOut.BufOut, err = cryptoKit.DecryptFrom(
					opArgs.BufIn, 
					opArgs.PeerKey,
					opKey.PrivKey)

			case ski.CryptOp_EXPORT_USING_PW:
				opOut.BufOut, err = cryptoKit.EncryptUsingPassword(
					crypto_rand.Reader, 
					opArgs.BufIn, 
					opArgs.PeerKey)

			case ski.CryptOp_IMPORT_USING_PW:
				opOut.BufOut, err = cryptoKit.DecryptUsingPassword(
					opArgs.BufIn, 
					opArgs.PeerKey)
				
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

			case ski.CryptOp_IMPORT_FROM_PEER,
				 ski.CryptOp_IMPORT_USING_PW: {
				newTome := ski.KeyTome{}
				err = newTome.Unmarshal(opOut.BufOut)
				ski.Zero(opOut.BufOut)
				ski.Zero(opArgs.PeerKey)

				if err == nil {
					sess.keyTomeMgr.MergeTome(&newTome)
					sess.bumpAutoSave()
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
 
func (sess *Session) bumpAutoSave() {

	sess.autoSaveMutex.Lock()
	sess.nextAutoSave = time.Now().Add(1600 * time.Millisecond)
	if sess.autoSave == nil {
		sess.autoSave = time.NewTicker(500 * time.Millisecond)
		go func() {
			for t := range sess.autoSave.C {
				sess.autoSaveMutex.Lock()
				saveNow := t.After(sess.nextAutoSave)
				sess.autoSaveMutex.Unlock()
				if saveNow {
					sess.saveToFile()
					break
				}
			}
		}()
	}
	sess.autoSaveMutex.Unlock()
	
}