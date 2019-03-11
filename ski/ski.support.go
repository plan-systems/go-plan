package ski

import (
	"bytes"
	"hash"
    "fmt"
	"io"
	"sort"
	"sync"

	//"crypto/rand"
    log "github.com/sirupsen/logrus"

	"golang.org/x/crypto/sha3"

	"github.com/plan-systems/go-plan/plan"
)

const (

	// MinPubKeyPrefixSz prevents suspiciously small pub key prefixes from being used.
	MinPubKeyPrefixSz = 16
)

// HashKit is an abstraction for hash.Hash
type HashKit struct {
	HashKitID HashKitID
	Hasher    hash.Hash
	HashSz    int
}

// ByKeyringName implements sort.Interface to sort a slice of Keyrings by binary name.
type ByKeyringName []*Keyring

func (a ByKeyringName) Len() int           { return len(a) }
func (a ByKeyringName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyringName) Less(i, j int) bool { return bytes.Compare(a[i].Name, a[j].Name) < 0 }

// ByNewestKey implements sort.Interface based on KeyEntry.TimeCreated
type ByNewestKey []*KeyEntry

func (a ByNewestKey) Len() int           { return len(a) }
func (a ByNewestKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByNewestKey) Less(i, j int) bool { return a[i].KeyInfo.TimeCreated > a[j].KeyInfo.TimeCreated }


// CompareKeyInfo fully compares two KeyInfos, sorting first by PubKey, then TimeCreated such that
//  ewer keys will appear first (descending TimeCreated)
//
// If 0 is returned, a and b are identical.
func CompareKeyInfo(a, b *KeyInfo) int {

	diff := bytes.Compare(a.PubKey, b.PubKey)

	// If pub keys are equal, ensure newer keys to the left
	if diff == 0 {
		diff = int(b.TimeCreated - a.TimeCreated) // Reverse time for newer keys to appear first
		if diff == 0 {
			diff = int(a.KeyType - b.KeyType)
			if diff == 0 {
				diff = int(a.CryptoKit - b.CryptoKit)
			}
		}
	}

	return diff
}



// CompareKeyEntry fully compares two KeyEntrys.
//
// If 0 is returned, a and b are identical.
func CompareKeyEntry(a, b *KeyEntry) int {

	diff := CompareKeyInfo(a.KeyInfo, b.KeyInfo)

    if diff == 0 {
        diff = bytes.Compare(a.PrivKey, b.PrivKey)
    }

	return diff
}

// ByNewestPubKey implements sort.Interface based on KeyEntry.PubKey followed by TimeCreated.
// See CompareEntries() to see sort order.
// For keys that have the same PubKey, the newer (larger TimeCreated) keys will appear first.
type ByNewestPubKey []*KeyEntry

func (a ByNewestPubKey) Len() int           { return len(a) }
func (a ByNewestPubKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByNewestPubKey) Less(i, j int) bool { return CompareKeyEntry(a[i], a[j]) < 0 }

// KeyTomeMgr wraps ski.KeyTome, offering threadsafe access and easy serialization.
type KeyTomeMgr struct {
	mutex   sync.RWMutex
	keyTome KeyTome
}

// NewKeyTomeMgr creates a new KeyTomeMgr
func NewKeyTomeMgr() *KeyTomeMgr {

	return &KeyTomeMgr{
		keyTome: KeyTome{
			Rev: 1,
		},
	}
}

// Clear resets this KeyHive as if NewKeyHive() was called instead, also zeroing out all private keys.
//
// THREADSAFE
func (mgr *KeyTomeMgr) Clear() {

	// TODO: zero out each key entry to protect private keys
	mgr.mutex.Lock()
	keySets := mgr.keyTome.Keyrings
	for _, keySet := range keySets {
		keySet.ZeroOut()
	}
	mgr.mutex.Unlock()

}

// FetchKey returns the first KeyEntry in the specified key set with a matching pub key prefix.
//
// If len(inPubKeyPrefix) == 0, then the newest key on the specified is returned.
//
// THREADSAFE
func (mgr *KeyTomeMgr) FetchKey(
	inKeyringName  []byte,
	inPubKeyPrefix []byte,
) (*KeyEntry, error) {

	var (
		match *KeyEntry
		err   error
	)

	mgr.mutex.RLock()

	kr := mgr.keyTome.FetchKeyring(inKeyringName)
	if kr == nil || len(kr.Keys) == 0 {
		err = plan.Errorf(nil, plan.KeyringNotFound, "keyring %v not found", inKeyringName)
	} else if len(inPubKeyPrefix) == 0 {
		match = kr.FetchNewestKey()
    } else {
		match = kr.FetchKeyWithPrefix(inPubKeyPrefix)
	}

	mgr.mutex.RUnlock()

	if match == nil && err == nil {
		err = plan.Errorf(nil, plan.KeyEntryNotFound, "pub key prefix %v not found in keyring %v", inPubKeyPrefix, inKeyringName)
	}

	return match, err
}

// ExportUsingGuide -- see ski.KeyTome.ExportUsingGuide()
func (mgr *KeyTomeMgr) ExportUsingGuide(
	inGuide *KeyTome,
	inOpts ExportKeysOptions,
) ([]byte, error) {

	mgr.mutex.RLock()
	buf, err := mgr.keyTome.ExportUsingGuide(inGuide, inOpts)
	mgr.mutex.RUnlock()

	return buf, err
}

// MergeTome merges the given tome into this tome, moving entries from ioSrc.
// If there is a KeyEntry.PubKey collision, the incoming key will remain in ioSrc
//
// THREADSAFE
func (mgr *KeyTomeMgr) MergeTome(
	ioSrc *KeyTome,
) {

	mgr.mutex.RLock()
	mgr.keyTome.MergeTome(ioSrc)
	mgr.mutex.RUnlock()
}

// Marshal writes out entire state to a given buffer.
// Warning: the return buffer is not encrypted and contains private key data!
//
// THREADSAFE
func (mgr *KeyTomeMgr) Marshal() ([]byte, error) {

	mgr.mutex.RLock()
	dAtA, err := mgr.keyTome.Marshal()
	mgr.mutex.RUnlock()

	return dAtA, err
}

// Unmarshal first calls ZeroOut() on itself and then performs deserializaton.
//
// THREADSAFE
func (mgr *KeyTomeMgr) Unmarshal(dAtA []byte) error {

	mgr.mutex.Lock()
	for _, keySet := range mgr.keyTome.Keyrings {
		keySet.ZeroOut()
	}
	err := mgr.keyTome.Unmarshal(dAtA)
	mgr.mutex.Unlock()

	return err
}

// ZeroOut zeros out the private key field of each contained key and resets the length of Entries.
func (kr *Keyring) ZeroOut() {

	for _, entry := range kr.Keys {
		entry.ZeroOut()
	}

	kr.Keys = kr.Keys[:0]
	kr.NewestPubKey = nil
}

// Resort resorts this Keyring's keys for speedy searching
func (kr *Keyring) Resort() {
	sort.Sort(ByNewestPubKey(kr.Keys))
	kr.SortedByPubKey = true
}

// MergeKeys is similar to MergeTome(), this consumes entries from srcKeyring and inserts them this this Keyring.
// Returns the number of KeyEntries added and resorts this Keyring.
//
// Post: len(srcKeyring.Keys) == 0
//
// Exact dupes are detected and ignored.
func (kr *Keyring) MergeKeys(srcKeyring *Keyring) int {

	newest := kr.FetchNewestKey()

	// First, detect and skip dupes
	N := len(srcKeyring.Keys)
	for i := 0; i < N; i++ {
		srcEntry := srcKeyring.Keys[i]
        keyInfo := srcEntry.KeyInfo

		// If we detect a dupe or a bad PubKey, skip it and act as if it's already added
		match := kr.FetchKeyWithPrefix(keyInfo.PubKey)
		if match != nil && (CompareKeyEntry(match, srcEntry) == 0 || len(keyInfo.PubKey) < MinPubKeyPrefixSz) {
			N--
			srcKeyring.Keys[i] = srcKeyring.Keys[N]
			i--
		} else {
			if newest == nil {
				newest = srcEntry
			} else if keyInfo.TimeCreated >= newest.KeyInfo.TimeCreated {
				newest = srcEntry
			}
		}
	}

	// This maintains the latest pub key
	if newest != nil {
		kr.NewestPubKey = newest.KeyInfo.PubKey
	} else {
		kr.NewestPubKey = nil
	}

	if N > 0 {
		kr.Keys = append(kr.Keys, srcKeyring.Keys[:N]...)
		kr.Resort()
    }
    
	srcKeyring.Keys = srcKeyring.Keys[:0]

	return N
}

/*

// ExportKeyringUsingGuide returns a new Keyring containing
func (kr *Keyring) ExportKeyringUsingGuide(
    inGuide *Keyring,
    inOpts ExportKeysOptions,
) (*Keyring, error) {

    newkr := &Keyring{
        Name: kr.Name,
    }

    N := len(inGuide.Keys)

    // If the guide is empty, export every key this Keyring
    if N == 0 {
        if len(kr.Keys) > 0 {
            newkr.Keys = make([]*KeyEntry, 0, len(kr.Keys))

            if (inOpts & IncludePrivateKey) != 0 {
                newkr.Keys = append(newkr.Keys, kr.Keys...)
            } else {
                for _, srcEntry := range kr.Keys {
                    entry := &KeyEntry{
                        KeyType:     srcEntry.KeyType,
                        CryptoKit: srcEntry.CryptoKit,
                        TimeCreated: srcEntry.TimeCreated,
                        PubKey:      srcEntry.PubKey,
                    }

                    newkr.Keys = append(newkr.Keys, entry)
                }
            }
        }
    } else {
        count := 0
        newkr.Keys = make([]*KeyEntry, 0, len(inGuide.Keys))

        for _, entry := range inGuide.Keys {
            match := kr.FetchKeyWithPrefix(entry.PubKey)

            if match == nil {
                if (inOpts & ErrorOnKeyNotFound) != 0 {
                    return nil, plan.Errorf(nil, plan.KeyEntryNotFound, "key %v not found to export", srcEntry.PubKey)
                }
            } else {
                newkr.Key[count] = ioGuide.Key[i]
                count++

                // Copy all the fields over
                entry = *match

                if (inOpts & IncludePrivateKey) == 0 {
                    entry.PrivKey = nil
                }
            }
        }

        ioGuide.Keys = ioGuide.Keys[:count]
    }

    ioGuide.NewestPubKey = nil
    ioGuide.Resort()

}
*/

/*

func (kr *Keyring) ExportWithGuide(
    ioGuide *Keyring,
    inOpts ExportKeysOptions,
) error {

    N := len(ioGuide.Keys)

    // If the guide is empty, export every key this Keyring
    if N == 0 {
        if len(kr.Keys) > 0 {
            ioGuide.Keys = make([]*KeyEntry, 0, len(kr.Keys))

            if (inOpts & IncludePrivateKey) != 0 {
                ioGuide.Keys = append(ioGuide.Keys, kr.Keys...)
            } else {
                for srcEntry := range kr.Keys {
                    entry := &KeyEntry{
                        KeyType:     srcEntry.KeyType,
                        CryptoKit: srcEntry.CryptoKit,
                        TimeCreated: srcEntry.TimeCreated,
                        PubKey:      srcEntry.PubKey,
                    }

                    ioGuide.Keys = append(ioGuide.Keys, entry)
                }
            }
        }
    } else {
        count := 0

        for i := 0; i < N; i++ {
            entry := ioGuide.Keys[i]
            match := kr.FetchKeyWithPrefix(entry.PubKey)

            if match == nil {
                if (inOpts & ErrorOnKeyNotFound)  != 0 {
                    return nil, plan.Errorf(nil, plan.KeyNotFound, "key %v not found to export", srcEntry.PubKey)
                }
            } else {
                ioGuide.Keys[count] = entry
                count++

                // Copy all the fields over
                *entry = *match

                if (inOpts & IncludePrivateKey) == 0 {
                    entry.PrivKey = nil
                }
            }
        }

        ioGuide.Keys = ioGuide.Keys[:count]
    }

    ioGuide.NewestPubKey = nil
    ioGuide.Resort()

}
*/

// FetchKeyWithPrefix returns the KeyEntry in this Keyring with a matching prefix.
//
// O(log n) if SortedByPubKey is set, O(n) otherwise.
func (kr *Keyring) FetchKeyWithPrefix(
	inPubKeyPrefix []byte,
) *KeyEntry {

	N := len(kr.Keys)
	pos := 0

	if kr.SortedByPubKey && N > 3 {
		pos = sort.Search(N,
			func(i int) bool {
				return bytes.Compare(kr.Keys[i].KeyInfo.PubKey, inPubKeyPrefix) >= 0
			},
		)
	}

	for ; pos < N; pos++ {
		entry := kr.Keys[pos]
		if bytes.HasPrefix(entry.KeyInfo.PubKey, inPubKeyPrefix) {
			return entry
		}
	}

	return nil
}

// FetchNewestKey returns the KeyEntry with the largest TimeCreated
func (kr *Keyring) FetchNewestKey() *KeyEntry {

	var newest *KeyEntry

	if len(kr.Keys) > 0 {

		if len(kr.NewestPubKey) > 0 {
			newest = kr.FetchKeyWithPrefix(kr.NewestPubKey)
		} else {
			for _, key := range kr.Keys {
				if newest == nil {
					newest = key
				} else if key.KeyInfo.TimeCreated > newest.KeyInfo.TimeCreated {
					newest = key
				}
			}
		}
	}

	return newest
}

// ExportKeysOptions is used with ExportWithGuide()
type ExportKeysOptions uint32
const (

	// ErrorOnKeyNotFound - if set, the export attempt will return an error if a given key was not found.   Otherwise, the entry is skipped/dropped.
	ErrorOnKeyNotFound = 1 << iota
)


// ZeroOut zeros out the private key field of each key in each key set
func (tome *KeyTome) ZeroOut() {

	for _, keySet := range tome.Keyrings {
		keySet.ZeroOut()
	}
}


// FetchKeyring returns the named Keyring (or nil if not found).
//
// O(log n) if SortedByName is set, O(n) otherwise.
func (tome *KeyTome) FetchKeyring(
	inKeyringName []byte,
) *Keyring {

	N := len(tome.Keyrings)
	pos := 0

	if tome.SortedByName && N > 3 {
		pos = sort.Search(N,
			func(i int) bool {
				return bytes.Compare(tome.Keyrings[i].Name, inKeyringName) >= 0
			},
		)
	}

	for ; pos < N; pos++ {
		kr := tome.Keyrings[pos]
		if bytes.Compare(kr.Name, inKeyringName) == 0 {
			return kr
		}
	}

	return nil
}



// ExportUsingGuide walks through inGuide and for each Keyring.Name + KeyEntry.PubKey match, the KeyEntry fields
//    are copied to a new KeyTome.  When complete, the new KeyTome is marshalled into an output buffer and returned.
//
// Note: Only Keyring.Name and KeyEntry.PubKey are used from ioGuide (other fields are ignored).
//
// Warning: since the returned buffer contains private key bytes, one should zero the result buffer after using it.
func (tome *KeyTome) ExportUsingGuide(
	inGuide *KeyTome,
	inOpts ExportKeysOptions,
) ([]byte, error) {

	outTome := &KeyTome{
		Rev:      tome.Rev,
		Keyrings: make([]*Keyring, 0, len(inGuide.Keyrings)),
	}

	for _, krGuide := range inGuide.Keyrings {

		krSrc := tome.FetchKeyring(krGuide.Name)
		if krSrc == nil {
			if (inOpts & ErrorOnKeyNotFound) != 0 {
				return nil, plan.Errorf(nil, plan.KeyringNotFound, "keyring %v not found to export", krGuide.Name)
			}
		} else {

			// If the guide Keyring is empty, that means export the whole keyring
			if len(krGuide.Keys) == 0 {
				outTome.Keyrings = append(outTome.Keyrings, krSrc)
			} else {
				newkr := &Keyring{
					Name: krSrc.Name,
					Keys: make([]*KeyEntry, 0, len(krGuide.Keys)),
				}
				outTome.Keyrings = append(outTome.Keyrings, newkr)

				for _, entry := range krGuide.Keys {
					match := krSrc.FetchKeyWithPrefix(entry.KeyInfo.PubKey)

					if match == nil {
						if (inOpts & ErrorOnKeyNotFound) != 0 {
							return nil, plan.Errorf(nil, plan.KeyEntryNotFound, "key %v not found to export", entry.KeyInfo.PubKey)
						}
					} else {
						newkr.Keys = append(newkr.Keys, match)
					}
				}
			}
		}
	}

	return outTome.Marshal()
}


// MergeTome merges the given tome into this tome, moving entries from ioSrc.
// If there is a KeyEntry duplicate, the key is ignored and will remain in inSrc
func (tome *KeyTome) MergeTome(
	srcTome *KeyTome,
) error {

	tome.Rev++

	// Ensure better Keyring search performance
	if ! tome.SortedByName {
		tome.ResortKeyrings()
	}

	// First, merge Keyrings that already exist (to leverage a binary search)
	keyringsToAdd := len(srcTome.Keyrings)
	for i := 0; i < keyringsToAdd; i++ {
		krSrc := srcTome.Keyrings[i]

		krDst := tome.FetchKeyring(krSrc.Name)
		if krDst == nil {
			continue
		}

        // TODO: handle generate keys pub_key collision (1:2^128 chance for SYM keys)
		krDst.MergeKeys(krSrc)
		keyringsToAdd--
		srcTome.Keyrings[i] = srcTome.Keyrings[keyringsToAdd]
		i--
	}

	if keyringsToAdd > 0 {

		// For each new Keyring that we're about to add, make sure it's prim and proper (don't trust incoming flags)
		for i := 0; i < keyringsToAdd; i++ {
			krSrc := srcTome.Keyrings[i]

			krSrc.Resort()
			krSrc.NewestPubKey = nil
			newest := krSrc.FetchNewestKey()
			if newest != nil {
				krSrc.NewestPubKey = newest.KeyInfo.PubKey
			}
		}

		// Finally, add the Keyrings that didn't already exist so that we only have to do one final sort
		tome.Keyrings = append(tome.Keyrings, srcTome.Keyrings[:keyringsToAdd]...)
		tome.ResortKeyrings()
	}

    srcTome.Keyrings = srcTome.Keyrings[:0]
}

// ResortKeyrings resorts all the contained Keyrings using ByKeyringName()
func (tome *KeyTome) ResortKeyrings() {
    sort.Sort(ByKeyringName(tome.Keyrings))
    tome.SortedByName = true
}

// GenerateFork returns a new KeyTome identical to this KeyTome, but with newly generated PubKey/PrivKey pairs.
// For each generated key, each originating KeyEntry's fields are reset (except for PrivKey which is set to to nil)
func (tome *KeyTome) GenerateFork(
	ioRand io.Reader,
	inRequestedKeyLen int,
) (*KeyTome, error) {

	tome.Rev++

	timeCreated := plan.Now().UnixSecs

	var kit *CryptoKit
	var err error

	newTome := &KeyTome{
		Rev:      1,
		Keyrings: make([]*Keyring, 0, len(tome.Keyrings)),
	}

	for _, krSrc := range tome.Keyrings {
		krDst := &Keyring{
			Name: krSrc.Name,
			Keys: make([]*KeyEntry, len(krSrc.Keys)),
		}
		newTome.Keyrings = append(newTome.Keyrings, krDst)

		for i, srcEntry := range krSrc.Keys {
            srcInfo := srcEntry.KeyInfo

			if kit == nil || kit.CryptoKitID != srcInfo.CryptoKit {
				kit, err = GetCryptoKit(srcInfo.CryptoKit)
				if err != nil {
					return nil, err
				}
			}

			newEntry := &KeyEntry{
                KeyInfo: &KeyInfo{
                    KeyType:     srcInfo.KeyType,
                    CryptoKit:   kit.CryptoKitID,
                    TimeCreated: timeCreated,
                },
			}

			err = kit.GenerateNewKey(
				ioRand,
				inRequestedKeyLen,
				newEntry,
			)
			if err != nil {
				return nil, err
			}
			if srcInfo.KeyType != newEntry.KeyInfo.KeyType || kit.CryptoKitID != newEntry.KeyInfo.CryptoKit {
				return nil, plan.Error(nil, plan.KeyGenerationFailed, "generate key altered key type")
			}

			krDst.Keys[i] = newEntry

            *srcInfo = *newEntry.KeyInfo
		}
	}

	return newTome, nil
}

// EqualTo compares if two key entries are identical/interchangable
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
    a := entry.KeyInfo
    b := entry.KeyInfo

	return a.KeyType != b.KeyType ||
		a.CryptoKit != b.CryptoKit ||
		a.TimeCreated != b.TimeCreated ||
		bytes.Equal(a.PubKey, b.PubKey) == false ||
		bytes.Equal(entry.PrivKey, other.PrivKey) == false

}

// ZeroOut zeros out this entry's private key buffer
func (entry *KeyEntry) ZeroOut() {
	N := int32(len(entry.PrivKey))
	for i := int32(0); i < N; i++ {
		entry.PrivKey[i] = 0
	}
}

// DebugDesc returns a human readable desc string for this KeyRef
func (kr *KeyRef) DebugDesc() string {
    return fmt.Sprintf("key %s on keyring %s", BinDesc(kr.PubKey), BinDesc(kr.KeyringName)) 
}

// Zero zeros out a given slice
func Zero(buf []byte) {
	N := int32(len(buf))
	for i := int32(0); i < N; i++ {
		buf[i] = 0
	}
}

// NewHashKit returns the requested HashKit.
func NewHashKit(inID HashKitID) (HashKit, error) {

	var kit HashKit

	if inID == 0 {
		inID = HashKitID_LegacyKeccak_256
	}

	kit.HashKitID = inID

	switch inID {

	case HashKitID_LegacyKeccak_256:
		kit.Hasher = sha3.NewLegacyKeccak256()

	case HashKitID_LegacyKeccak_512:
		kit.Hasher = sha3.NewLegacyKeccak512()

	case HashKitID_SHA3_256:
		kit.Hasher = sha3.New256()

	case HashKitID_SHA3_512:
		kit.Hasher = sha3.New512()

	default:
		return HashKit{}, plan.Errorf(nil, plan.HashKitNotFound, "failed to recognize HashKitID %v", inID)
	}

	kit.HashSz = kit.Hasher.Size()

	return kit, nil
}

/*
// GenerateNewKeys is a convenience bulk function for CryptoKit.GenerateNewKey()
func GenerateNewKeys(
	inRand io.Reader,
	inRequestedKeyLen int,
	inKeySpecs []*KeyEntry,
) ([]*KeyEntry, error) {

	N := len(inKeySpecs)

	newKeys := make([]*KeyEntry, N)

	var kit *CryptoKit
	var err error

	timeCreated := plan.Now().UnixSecs

	for i, keySpec := range inKeySpecs {

		if kit == nil || kit.CryptoKit != keySpec.CryptoKit {
			kit, err = GetCryptoKit(keySpec.CryptoKit)
			if err != nil {
				return nil, err
			}
		}

		newKey := &KeyEntry{
			KeyType:     keySpec.KeyType,
			CryptoKit: kit.CryptoKit,
			TimeCreated: timeCreated,
		}

		err = kit.GenerateNewKey(
			inRand,
			inRequestedKeyLen,
			newKey,
		)
		if err != nil {
			return nil, err
		}

		newKeys[i] = newKey
	}

	return newKeys, nil
}
*/

// GenerateNewKey creates a new key, blocking until completion
func GenerateNewKey(
    inSession Session,
	inKeyringName []byte,
    inKeyInfo KeyInfo,
) (*KeyInfo, error) {

	tomeOut, err := inSession.GenerateKeys(&KeyTome{
        Keyrings: []*Keyring{
            &Keyring{
                Name: inKeyringName,
                Keys: []*KeyEntry{
                    &KeyEntry{
                        KeyInfo: &inKeyInfo,
                    },
                },
            },
        },
	})

	var kr *Keyring
	if err == nil && tomeOut != nil && tomeOut.Keyrings[0] != nil {
		kr = tomeOut.Keyrings[0]
	}

	if kr == nil || kr.Keys[0] == nil || kr.Keys[0].KeyInfo == nil {
		return nil, plan.Error(nil, plan.AssertFailed, "no keys returned")
	}

	if kr.Keys[0].KeyInfo.KeyType !=  inKeyInfo.KeyType {
		return nil, plan.Error(nil, plan.AssertFailed, "unexpected key type")
	}

	if ! bytes.Equal(inKeyringName, kr.Name) {
		return nil, plan.Error(nil, plan.AssertFailed, "generate returned different keyring name")
	}

    return kr.Keys[0].KeyInfo, nil

}

/*
const (
    TIDTimestampSz = 6
    TID
)

func GenerateEpochID() []byte {

}*/


// SessionTool is a small set of util functions for creating a SKI session.
type SessionTool struct {
	UserID        string
	Session       Session
	CryptoKitID   CryptoKitID
	CommunityKey  KeyRef
	P2PKey        KeyRef
}

// NewSessionTool creates a new tool for helping manage a SKI session.
func NewSessionTool(
	inProvider    CryptoProvider,
	inUserID      string,
	inCommunityID []byte,
) (*SessionTool, error) {

	st := &SessionTool{
		UserID: inUserID,
		CommunityKey: KeyRef{
			KeyringName: inCommunityID,
		},
        P2PKey: KeyRef{
            KeyringName: append([]byte(inUserID), inCommunityID...),
        },
	}

    var err error

	st.Session, err = inProvider.StartSession(SessionParams{
		Invocation: plan.Block{
			Label: inProvider.InvocationStr(),
		},
	})
	if err != nil {
		return nil, err
	}

    err = st.GetLatestKey(&st.P2PKey, KeyType_ASYMMETRIC_KEY)
	if err != nil {
		return nil, err
	}

	return st, nil
}


// DoOp performs the given op, blocking until completion
func (st *SessionTool) DoOp(inArgs CryptOpArgs) ([]byte, error) {

	out, err := st.Session.DoCryptOp(&inArgs)
	if err != nil {
		return nil, err
	}

	return out.BufOut, nil
}


// GetLatestKey updates the given KeyRef with the newest pub key on a given keyring (using ioKeyRef.KeyringName)
func (st *SessionTool) GetLatestKey(
    ioKeyRef    *KeyRef, 
    inAutoCreate KeyType,
) error {

    ioKeyRef.PubKey = nil
    
    keyInfo, err := st.Session.FetchKeyInfo(ioKeyRef)
    if plan.IsError(err, plan.KeyringNotFound, plan.KeyEntryNotFound) {
        if inAutoCreate != KeyType_Unspecified {
            keyInfo, err = GenerateNewKey(
                st.Session,
                ioKeyRef.KeyringName,
                KeyInfo{
                    KeyType:inAutoCreate,
                    CryptoKit: st.CryptoKitID,
                },
            )
            if err == nil {
                log.Infof("%10s: created %v %v", st.UserID, KeyType_name[int32(inAutoCreate)], BinDesc(keyInfo.PubKey))
            }
        }
    }
	if err != nil {
		return err
	}
    
    ioKeyRef.PubKey = keyInfo.PubKey

    return nil
}


// EndSession ends the current session
func (st *SessionTool) EndSession(inReason string) {

	st.Session.EndSession(inReason)

}

// FormKeyringNameForMember forms a keyring name (ski.Keyring.KeyringName) for the given member ID
func FormKeyringNameForMember(
    memberID    plan.MemberID,
	communityID []byte,
) []byte {

    clen := len(communityID)
    if clen < plan.MemberIDSz {
        clen = plan.MemberIDSz
    }
    krName := make([]byte, clen)
    copy(krName, communityID)

    for i := 0 ; i < plan.MemberIDSz; i++ {
        krName[i] ^= byte(memberID)
        memberID >>= 8
    }

    return krName
}


// BinDesc returns a base64 encoding of a binary string, limiting it to a short number of character for debugging and logging.
func BinDesc(inBinStr []byte) string {

    if len(inBinStr) == 0 {
        return "nil"
    }

    binStr := inBinStr

    const limit = 12
    alreadyASCII := true
    for _, b := range binStr {
        if b < 32 || b > 126 {
            alreadyASCII = false
            break
        }
    }

    suffix := ""
    if len(binStr) > limit {
        binStr = binStr[:limit]
        suffix = "[…]"
    }

    outStr := ""
    if alreadyASCII {
        outStr = string(binStr)
    } else {
        outStr = plan.Base64.EncodeToString(binStr)
    }

    return outStr + suffix
}


/*
// FormMemberID returns a pseudo-random hash from a given seed string and community ID.
func FormMemberID(
    inSeedStr   string,
    inEpoch     *pdi.StorageEpoch,
) uint64 {

    kit, _ := NewHashKit(HashKitID_LegacyKeccak_256)
    kit.Hasher.Reset()
    kit.Hasher.Write(inEpoch.CommunityId)
    kit.Hasher.Write(inEpoch.EpochId)
    kit.Hasher.Write([]byte(inSeedStr))

    var buf [64]byte
    kit.Hasher.Sum(buf[:0])


    var memID uint64
    for i := 0; i < 8; i++ {
        memID = (memID << 8) | uint64(buf[i])
    }

    return memID
}*/



// PayloadPacker signs and packs payload buffers IAW ski.SigHeader
type PayloadPacker struct {
    signSession Session
	hashKit        HashKit
    signingKeyRef *KeyRef
    signingKey     KeyInfo
    threadsafe     bool
    mutex          sync.Mutex
}


// NewPacker creates a new PayloadSigner
func NewPacker(
    inMakeThreadsafe bool,
) PayloadPacker {

	return PayloadPacker{
        threadsafe:    inMakeThreadsafe,
        signingKeyRef: &KeyRef{},
	}
}


// ResetSigner --see TxnEncoder
func (P *PayloadPacker) ResetSigner(
	inSession        Session,
	inSigningKey     KeyRef,
    inPayloadHashKit HashKitID,
    outKeyInfo      *KeyInfo,
) error {

    P.signSession = nil
    P.signingKeyRef.KeyringName = nil
    P.signingKey.CryptoKit = 0

    keyEntry, err := inSession.FetchKeyInfo(&inSigningKey)
    if err != nil {
        return err
    }
    if keyEntry.KeyType != KeyType_SIGNING_KEY {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "not a signing key")
    }

	P.hashKit, err = NewHashKit(inPayloadHashKit)
	if err != nil {
		return err
	}

    P.signSession = inSession

    P.signingKeyRef.KeyringName = inSigningKey.KeyringName
    P.signingKeyRef.PubKey      = keyEntry.PubKey

    P.signingKey.KeyType   = keyEntry.KeyType
    P.signingKey.CryptoKit = keyEntry.CryptoKit
    P.signingKey.PubKey    = keyEntry.PubKey

    if outKeyInfo != nil {
        *outKeyInfo = P.signingKey
    }

    return P.checkReady()
}

// SignAndPack signs a hash digest and packages it along with the payload and encodinginto into a single composite buffer
//    that is indented to be decoded via PayloadUnpacker.UnpackAndVerify()
//
// THREADSAFE
func (P *PayloadPacker) SignAndPack(
    inPayload     []byte,
    inPayloadEnc  plan.Encoding,
    inExtraAlloc  int,
) ([]byte, []byte, error) {

    err := P.checkReady()
    if err != nil {
        return nil, nil, err
    }

    hdr := SigHeader{
        SignerCryptoKit: P.signingKey.CryptoKit,
        SignerPubKey:    P.signingKey.PubKey,
        PayloadHashKit:  P.hashKit.HashKitID,
        PayloadEncoding: inPayloadEnc,
        PayloadSz:       int64(len(inPayload)),
    }

    // Note that hdr doesn't yet have the sig in it, so add extra for that and for the hash digest size
    extra := inExtraAlloc + P.hashKit.HashSz
    bufSz := hdr.Size() + 300 + int(hdr.PayloadSz) + extra
    buf := make([]byte, bufSz)

    if P.threadsafe {
        P.mutex.Lock()
    }

    P.hashKit.Hasher.Reset()
    P.hashKit.Hasher.Write(inPayload)
    extraPos := bufSz-extra
    payloadHash := P.hashKit.Hasher.Sum(buf[extraPos:extraPos])

    if len(payloadHash) != P.hashKit.HashSz {
        return nil, nil, plan.Error(nil, plan.AssertFailed, "hasher returned bad digest length")
    }
    
    if P.threadsafe {
        P.mutex.Unlock()
    }

    out, err := P.signSession.DoCryptOp(&CryptOpArgs{
        CryptOp: CryptOp_SIGN,
        BufIn: payloadHash,
        OpKey: P.signingKeyRef,
    })
    if err != nil {
        return nil, nil, err
    }
      
    hdr.Sig = out.BufOut


    // 1) Append the marshalled SigHeader
    var hdrSz int
    hdrSz, err = hdr.MarshalTo(buf[2:])
    if err != nil {
        return nil, nil, plan.Error(err, plan.MarshalFailed, "failed to marshal SigHeader")
    }
    buf[0] = byte(hdrSz)
    buf[1] = byte(hdrSz >> 8)
    pos := int64(hdrSz) + 2

    // 2) Append the payload buf
    copy(buf[pos:pos + hdr.PayloadSz], inPayload)
    pos += hdr.PayloadSz

    return buf[:pos], payloadHash, nil
}


// checkReady checks if everything is in place to perform SignAndPack(). 
//
// THREADSAFE
func (P *PayloadPacker) checkReady() error {

	if P.hashKit.Hasher == nil {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "payload hasher not set")
	}

	if P.signSession == nil {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "SKI signing session missing")
	}

	if len(P.signingKeyRef.KeyringName) == 0 {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "signer keyring name missing")
	}

    if P.signingKey.CryptoKit == 0 {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "signing key CryptoKit not set")
    }

	return nil
}


// SignedPayload are the params associated with signing a payload buffer.
type SignedPayload struct {
    Payload         []byte        // Client payload
    PayloadEncoding plan.Encoding // Encoding of .Payload
    HashKit         HashKitID     // The ID of the hash kit that generated .Hash 
    Hash            []byte        // A hash digest generated from .Payload
    Sig             []byte        // Signature of .Hash by .Signer
    Signer          KeyInfo       // The pub key that orginated .Sig
}

// PayloadUnpacker unpacks and decodes signed buffers IAW ski.SigHeader
type PayloadUnpacker struct {
    threadsafe   bool
    mutex        sync.Mutex
	hashKits     map[HashKitID]HashKit
}

// NewUnpacker creates a new 
func NewUnpacker(
    inMakeThreadsafe bool,
) PayloadUnpacker {
    return PayloadUnpacker{
        threadsafe: inMakeThreadsafe,
		hashKits:   map[HashKitID]HashKit{},
    }
}


// UnpackAndVerify decodes the given buffer into its payload and signature components, and verifies the signature.
// This procedures assumes the signed buf was produced via Signer.SignAndPack()
// Note: they returned payload buffer is a slice of inSignedBuf.
func (U* PayloadUnpacker) UnpackAndVerify(
    inSignedBuf []byte,
    out *SignedPayload,
) error {

    var (
        err error
        hdr SigHeader
    )

    pos := uint32(0)

    bufLen := uint32(len(inSignedBuf))
    if bufLen < 30 {
		err = plan.Errorf(nil, plan.UnmarshalFailed, "signed buf is too small (len=%v)", bufLen)
    } 

	// 1) Unmarshal the txn info
    if err == nil {
        pos = 2 + uint32(inSignedBuf[0]) | ( uint32(inSignedBuf[1]) << 8 )
        if pos > bufLen {
            err = plan.Error(nil, plan.UnmarshalFailed, "payload pos exceeds buf size")
        }
    }

    if err == nil {
        err = hdr.Unmarshal(inSignedBuf[2:pos])
        if err != nil {
            err = plan.Error(err, plan.UnmarshalFailed, "failed to unmarshal ski.SigHeader")
        }
    }

    if err == nil {
        if U.threadsafe {
            U.mutex.Lock()
        }

        // 4) Prep the hasher so we can generate a digest
        hashKit, ok := U.hashKits[hdr.PayloadHashKit]
        if ! ok {
            var err error
            hashKit, err = NewHashKit(hdr.PayloadHashKit)
            if err == nil {
                U.hashKits[hdr.PayloadHashKit] = hashKit
            }
        }

        // 5) Calculate the hash digest and thus UTID of the raw txn
        if err == nil {
            hashKit.Hasher.Reset()
            hashKit.Hasher.Write(inSignedBuf[pos:])
            out.Hash = hashKit.Hasher.Sum(out.Hash[:0])
        }
        
        if U.threadsafe {
            U.mutex.Unlock()
        }
    }

	// 6) Verify the sig
    if err == nil {
	    err = VerifySignature(hdr.Sig, out.Hash, hdr.SignerCryptoKit, hdr.SignerPubKey)
	}

    out.Payload          = inSignedBuf[pos:]
    out.PayloadEncoding  = hdr.PayloadEncoding
    out.HashKit          = hdr.PayloadHashKit
    out.Sig              = hdr.Sig
    out.Signer.PubKey    = hdr.SignerPubKey
    out.Signer.KeyType   = KeyType_SIGNING_KEY
    out.Signer.CryptoKit = hdr.SignerCryptoKit

    return err
}