// Package hive implements ski.Provider for keys stored on the local file system (via encrypted file)
package hive

import (
	"io/ioutil"
	"os"
	"path"
	"time"

	//io"
	crypto_rand "crypto/rand"
	"sync"

	"github.com/plan-systems/plan-go/ctx"
	"github.com/plan-systems/plan-go/device"
	"github.com/plan-systems/plan-go/ski"

	// CryptoKits always available
	_ "github.com/plan-systems/plan-go/ski/CryptoKits/ed25519"
	_ "github.com/plan-systems/plan-go/ski/CryptoKits/nacl"
)

// hiveSess is an implementation of the SKI
type hiveSess struct {
	ski.Session
	ctx.Logger

	hiveCryptoKit ski.CryptoKit
	autoSaveMutex sync.Mutex
	nextAutoSave  time.Time
	StoreName     string
	BaseDir       string
	keyTomeMgr    *ski.KeyTomeMgr
	saveTicker    *time.Ticker
	savePending   bool
	hivePass      []byte
}

// StartSession starts a new hive SKI session locally, using a locally encrypted file.
// If len(BaseDir) == 0, then this session is heap only (and will be zeroed/lost when the session ends)
func StartSession(
	inBaseDir string,
	inStoreName string,
	inPass []byte,
) (ski.Session, error) {

	cryptoKit, err := ski.GetCryptoKit(ski.CryptoKitID_NaCl)
	if err != nil {
		return nil, err
	}

	sess := &hiveSess{
		Logger:        ctx.NewLogger("ski.hive"),
		hiveCryptoKit: cryptoKit,
		nextAutoSave:  time.Now(),
		BaseDir:       inBaseDir,
		StoreName:     inStoreName,
		keyTomeMgr:    ski.NewKeyTomeMgr(),
		hivePass:      make([]byte, 0, 64),
	}
	sess.hivePass = append(sess.hivePass, inPass...)

	if err = sess.loadFromFile(); err != nil {
		return nil, err
	}
	return sess, nil
}

func (sess *hiveSess) dbPathname() string {
	if len(sess.BaseDir) == 0 || len(sess.StoreName) == 0 {
		return ""
	}
	return path.Join(sess.BaseDir, sess.StoreName)
}

func (sess *hiveSess) loadFromFile() error {

	sess.autoSaveMutex.Lock()
	defer sess.autoSaveMutex.Unlock()

	sess.clearSave()

	doClear := true

	var err error

	pathname := sess.dbPathname()
	if len(pathname) > 0 {
		var buf []byte
		buf, err = ioutil.ReadFile(pathname)
		if err == nil {
			buf, err = sess.hiveCryptoKit.DecryptUsingPassword(buf, sess.hivePass)

			if err == nil && len(buf) > 0 {
				err = sess.keyTomeMgr.Unmarshal(buf)
				if err == nil {
					doClear = false
				}
			}
		} else {
			// If file doesn't exist, don't consider it an error
			if os.IsNotExist(err) {
				err = nil
			} else {
				err = ski.ErrCode_KeyHiveFailedToLoad.ErrWithMsgf("failed to load key hive %v", pathname)
			}
		}
		ski.Zero(buf)
	}

	if doClear {
		sess.keyTomeMgr.Clear()
	}
	return err
}

func (sess *hiveSess) saveToFile() error {
	sess.autoSaveMutex.Lock()
	defer sess.autoSaveMutex.Unlock()

	if sess.savePending {
		pathname := sess.dbPathname()
		if len(pathname) > 0 {
			buf, err := sess.keyTomeMgr.Marshal()

			if err == nil {
				buf, err = sess.hiveCryptoKit.EncryptUsingPassword(crypto_rand.Reader, buf, sess.hivePass)
				if err == nil {
					err = ioutil.WriteFile(pathname, buf, plan.DefaultFileMode)
					if err != nil {
						err = ski.ErrCode_KeyTomeFailedToSave.ErrWithMsgf("failed to save hive at %v", pathname)
					}
				}
			}

			// TODO: on key save failure, save to crash file?
			if err != nil {
				sess.Error("couldn't save to file: ", err)
				return err
			}
		}
		sess.clearSave()
	}
	return nil
}

func (sess *hiveSess) clearSave() {

	// When we save out successfully, stop the autosave goroutine
	{
		if sess.savePending {
			sess.savePending = false
			sess.saveTicker.Stop()
		}
	}
}

// EndSession -- see ski.Session
func (sess *hiveSess) EndSession(inReason string) {
	sess.saveToFile()
	ski.Zero(sess.hivePass)
}

// GenerateKeys -- see ski.Session
func (sess *hiveSess) GenerateKeys(srcTome *ski.KeyTome) (*ski.KeyTome, error) {

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
		return nil, ski.ErrCode_AssertFailed.ErrWithMsg("generated keys failed to merge")
	}

	sess.bumpAutoSave()

	return srcTome, nil
}

// FetchKeyInfo -- see ski.Session
func (sess *hiveSess) FetchKeyInfo(inKeyRef *ski.KeyRef) (*ski.KeyInfo, error) {
	opKey, err := sess.keyTomeMgr.FetchKey(inKeyRef.KeyringName, nil)
	if err != nil {
		return nil, err
	}

	return opKey.KeyInfo, nil
}

// DoCryptOp -- see ski.Session
func (sess *hiveSess) DoCryptOp(opArgs *ski.CryptOpArgs) (*ski.CryptOpOut, error) {
	var err error
	opOut := &ski.CryptOpOut{}

	/*****************************************************
	** 0) PRE-OP
	**/
	if err == nil {
		switch opArgs.CryptOp {

		case ski.CryptOp_ExportToPeer, ski.CryptOp_ExportUsingPw:
			{
				if opArgs.TomeIn == nil {
					err = ski.ErrCode_AssertFailed.ErrWithMsg("missing CryptOpArgs.TomeIn")
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
		opKey     *ski.KeyEntry
		cryptoKit ski.CryptoKit
	)
	if err == nil {
		switch opArgs.CryptOp {
		case ski.CryptOp_ExportUsingPw,
			ski.CryptOp_ImportUsingPw:
			cryptoKit, err = ski.GetCryptoKit(opArgs.DefaultCryptoKit)

		default:
			if opArgs.OpKey == nil {
				err = ski.ErrCode_AssertFailed.ErrWithMsg("missing CryptOpArgs.KeyRef")
			} else {
				opKey, err = sess.keyTomeMgr.FetchKey(opArgs.OpKey.KeyringName, opArgs.OpKey.PubKey)
				if err == nil {
					opOut.OpPubKey = opKey.KeyInfo.PubKey
					cryptoKit, err = ski.GetCryptoKit(opKey.KeyInfo.CryptoKitID)
				}
			}
		}
	}

	/*****************************************************
	** 2) DO OP
	**/
	if err == nil {
		switch opArgs.CryptOp {

		case ski.CryptOp_Sign:
			opOut.BufOut, err = cryptoKit.Sign(
				opArgs.BufIn,
				opKey.PrivKey)

		case ski.CryptOp_EncryptSym:
			opOut.BufOut, err = cryptoKit.Encrypt(
				crypto_rand.Reader,
				opArgs.BufIn,
				opKey.PrivKey)

		case ski.CryptOp_DecryptSym:
			opOut.BufOut, err = cryptoKit.Decrypt(
				opArgs.BufIn,
				opKey.PrivKey)

		case ski.CryptOp_EncryptToPeer,
			ski.CryptOp_ExportToPeer:
			opOut.BufOut, err = cryptoKit.EncryptFor(
				crypto_rand.Reader,
				opArgs.BufIn,
				opArgs.PeerKey,
				opKey.PrivKey)

		case ski.CryptOp_DecryptFromPeer,
			ski.CryptOp_ImportFromPeer:
			opOut.BufOut, err = cryptoKit.DecryptFrom(
				opArgs.BufIn,
				opArgs.PeerKey,
				opKey.PrivKey)

		case ski.CryptOp_ExportUsingPw:
			opOut.BufOut, err = cryptoKit.EncryptUsingPassword(
				crypto_rand.Reader,
				opArgs.BufIn,
				opArgs.PeerKey)

		case ski.CryptOp_ImportUsingPw:
			opOut.BufOut, err = cryptoKit.DecryptUsingPassword(
				opArgs.BufIn,
				opArgs.PeerKey)

		default:
			err = ski.ErrCode_UnrecognizedCryptOp.ErrWithMsgf("unrecognized SKI operation %v", opArgs.CryptOp)
		}
	}

	/*****************************************************
	** 3) POST OP
	**/
	if err == nil {
		switch opArgs.CryptOp {

		case ski.CryptOp_ExportToPeer:
			ski.Zero(opArgs.BufIn)

		case ski.CryptOp_ImportFromPeer,
			ski.CryptOp_ImportUsingPw:
			{
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

func (sess *hiveSess) bumpAutoSave() {

	sess.autoSaveMutex.Lock()
	sess.nextAutoSave = time.Now().Add(1600 * time.Millisecond)

	// If there's already a save pending/queued, then we can move on.
	if !sess.savePending {
		sess.savePending = true
		sess.saveTicker = time.NewTicker(500 * time.Millisecond)
		go func() {
			for t := range sess.saveTicker.C {
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