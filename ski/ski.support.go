package ski

import (
    "io"
	"bytes"
    "hash"
    "sync"
    "sort"
    //"crypto/rand"

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
func (a ByNewestKey) Less(i, j int) bool { return a[i].TimeCreated > a[j].TimeCreated }


// CompareEntries fully compares two KeyEntrys, sorting first by PubKey, then TimeCreated such that
//  ewer keys will appear first (descending TimeCreated) 
//
// If 0 is returned, a and b are identical.
func CompareEntries(a, b *KeyEntry) int {
   
    diff := bytes.Compare(a.PubKey, b.PubKey)

    // If pub keys are equal, ensure newer keys to the left
    if diff == 0 {
        diff = int(b.TimeCreated - a.TimeCreated)       // Reverse time for newer keys to appear first
        if diff == 0 {
            diff = int(a.KeyType - b.KeyType)
            if diff == 0 {
                diff = int(a.CryptoKitId - b.CryptoKitId)
                if diff == 0 {
                    diff = bytes.Compare(a.PrivKey, b.PrivKey)
                }
            }
        }
    }

    return diff
}



// ByNewestPubKey implements sort.Interface based on KeyEntry.PubKey followed by TimeCreated.
// See CompareEntries() to see sort order.
// For keys that have the same PubKey, the newer (larger TimeCreated) keys will appear first.
type ByNewestPubKey []*KeyEntry
func (a ByNewestPubKey) Len() int           { return len(a) }
func (a ByNewestPubKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByNewestPubKey) Less(i, j int) bool { return CompareEntries(a[i], a[j]) < 0 }



/*

// InsertEntry inserts the given KeyEntry, inserting such that next entry has a smaller TimeCreated (preserving order).
// If inEntry is an exact dupe, this function has no effect
//
// Pre: this Keyring is assumed to be sorted from newest to older keys (decreasing KeyEntry.TimeCreated)
func (kr *Keyring) InsertEntry(inEntry *KeyEntry) {
    
    i := int32(0)
    N := int32(len(kr.Keys))

    t := inEntry.TimeCreated

    for ; i < N && t < kr.Keys[i].TimeCreated; i++ {
    }

    pos := i

    // Don't insert if we detect a dupe
    for ; i < N && t == kr.Keys[i].TimeCreated; i++ {
        if kr.Keys[i].EqualTo(inEntry) {
            return
        }
    }

    kr.Keys = append(kr.Keys, nil)
    N++
    copy(kr.Keys[pos:N-1], kr.Keys[pos+1:N])
    kr.Keys[pos] = inEntry

}
*/


// KeyTomeMgr wraps ski.KeyTome, offering threadsafe access and easy serialization.
type KeyTomeMgr struct {

    mutex           sync.RWMutex
    keyTome         KeyTome
    
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
// THREADSAFE
func (mgr *KeyTomeMgr) FetchKey(
    inKeyringName  []byte,
    inPubKeyPrefix []byte,
) (*KeyEntry,  error) {

    var (
        match *KeyEntry
        err error
    )

    mgr.mutex.RLock()

    kr := mgr.keyTome.FetchKeyring(inKeyringName)
    if kr == nil || len(kr.Keys) == 0 {
        err = plan.Errorf(nil, plan.KeyringNotFound, "keyring %v not found", inKeyringName)
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



// ZeroOut zeros out the private key field of each key in each key set
func (tome *KeyTome) ZeroOut() {
    
    for _, keySet := range tome.Keyrings {
        keySet.ZeroOut()
    }
}

/*
func (tome *KeyTome) Op(
    inKeyringName []byte,
) *Keyring {


    if !  tome.SortedByName {
        sort.Sort(ByKeyringName(tome.Keyrings))
        tome.SortedByName = true
    }
    
func (tome *KeyTome) ResortByKey(
    inKeyringName []byte,
) *Keyring {

*/



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





// MergeKeys is similar to MergeTome(), this consumes entries from ioKeyring and inserts them this this Keyring.
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

        // If we detect a dupe or a bad PubKey, skip it and act as if it's already added
        match := kr.FetchKeyWithPrefix(srcEntry.PubKey)  
        if match != nil && (CompareEntries(match, srcEntry) == 0 || len(srcEntry.PubKey) < MinPubKeyPrefixSz) {
            N--
            srcKeyring.Keys[i] = srcKeyring.Keys[N]
            i--
        } else {
            if newest == nil {
                newest = srcEntry
            } else if srcEntry.TimeCreated >= newest.TimeCreated {
                newest = srcEntry
            }
        }
    }

    srcKeyring.Keys = srcKeyring.Keys[:0]

    // This maintains the latest pub key
    if newest != nil {
        kr.NewestPubKey = newest.PubKey
    } else {
        kr.NewestPubKey = nil
    }

    if N > 0 {
        kr.Keys = append(kr.Keys, srcKeyring.Keys[:N]...)
        kr.Resort()
    }

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
                        CryptoKitId: srcEntry.CryptoKitId,
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
                        CryptoKitId: srcEntry.CryptoKitId,
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
    PubKeyPrefix []byte,
) *KeyEntry {

    N := len(kr.Keys)
    pos := 0

    if kr.SortedByPubKey && N > 3 {
        pos = sort.Search(N,
            func (i int) bool {
                return bytes.Compare(kr.Keys[i].PubKey, PubKeyPrefix) >= 0
            },
        )
    }
    
    for ; pos < N; pos++ {
        entry := kr.Keys[pos]
        if bytes.HasPrefix(entry.PubKey, PubKeyPrefix) {
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
                } else if key.TimeCreated > newest.TimeCreated {
                    newest = key
                }
            }
        }
    }
        
    return newest
}


//
// if len(PubKeyPrefix) == 0, then Keyring.DefaultPubKey is used to lookup the key.


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
            func (i int) bool {
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


// ExportKeysOptions is used with ExportWithGuide() 
type ExportKeysOptions uint32
const (

    // ErrorOnKeyNotFound - if set, the export attempt will return an error if a given key was not found.   Otherwise, the entry is skipped/dropped.
    ErrorOnKeyNotFound = 1 << iota

)


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
        Rev: tome.Rev,
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
                    match := krSrc.FetchKeyWithPrefix(entry.PubKey)

                    if match == nil {
                        if (inOpts & ErrorOnKeyNotFound) != 0 {
                            return nil, plan.Errorf(nil, plan.KeyEntryNotFound, "key %v not found to export", entry.PubKey)
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

/*

  if len(kr.Keys) > 0 {
            newkr.Keys = make([]*KeyEntry, 0, len(kr.Keys))

            if (inOpts & IncludePrivateKey) != 0 {
                newkr.Keys = append(newkr.Keys, kr.Keys...)
            } else {
                for _, srcEntry := range kr.Keys {
                    entry := &KeyEntry{
                        KeyType:     srcEntry.KeyType,
                        CryptoKitId: srcEntry.CryptoKitId,
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
    }*/


// MergeTome merges the given tome into this tome, moving entries from ioSrc.
// If there is a KeyEntry duplicate, the key is ignored and will remain in inSrc
func (tome *KeyTome) MergeTome(
    srcTome *KeyTome,
) {
    
    tome.Rev++

    // Ensure better Keyring search performance
    if  ! tome.SortedByName {
        sort.Sort(ByKeyringName(tome.Keyrings))
        tome.SortedByName = true
    }

    // First, merge Keyrings that already exist (to leverage a binary search)
    keyringsToAdd := len(srcTome.Keyrings)
    for i := 0; i < keyringsToAdd; i++ {
        krSrc := srcTome.Keyrings[i]

        krDst := tome.FetchKeyring(krSrc.Name)
        if krDst == nil {
            continue
        }

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
                krSrc.NewestPubKey = newest.PubKey
            }
        }

        // Finally, add the Keyrings that didn't already exist so that we only have to do one final sort
        tome.Keyrings = append(tome.Keyrings, srcTome.Keyrings[:keyringsToAdd]...)
        sort.Sort(ByKeyringName(tome.Keyrings))
        tome.SortedByName = true
    }

}


/*
func (tome *KeyTome) MergeKeyring(
    ioSrc *KeyTome,
) {
    
    tome.Rev++

    for _, krSrc := range ioSrc.Keyrings {
        krDst := tome.FetchKeyringWithName(krSrc.Name)
        if krDst == nil {
            tome.Keyrings[krName] = krSrc
            delete(ioSrc.Keyrings, krName)
        } else {

            // TODO: just append the new entries, resort by TimeCreated, and use map to remove any dupes?
            for _, entry := range krSrc.Keys{
                krDst.InsertEntry(entry)
            }
            krSrc.Keys = nil 
        }
    }
}
*/



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
        Rev: 1,
        Keyrings: make([]*Keyring, 0, len(tome.Keyrings)),
    }

    for _, krSrc := range tome.Keyrings {
        krDst := &Keyring{
            Name: krSrc.Name,
            Keys: make([]*KeyEntry, len(krSrc.Keys)),
        }
        newTome.Keyrings = append(newTome.Keyrings, krDst)

        for i, srcEntry := range krSrc.Keys {

            if kit == nil || kit.CryptoKitID != srcEntry.CryptoKitId {
                kit, err = GetCryptoKit(srcEntry.CryptoKitId)
                if err != nil {
                    return nil, err
                }
            }

            newEntry := &KeyEntry{
                KeyType: srcEntry.KeyType,
                CryptoKitId: kit.CryptoKitID,
                TimeCreated: timeCreated,
            }

            err = kit.GenerateNewKey(
                ioRand,
                inRequestedKeyLen,
                newEntry,
            )
            if err != nil {
                return nil, err
            }
            if srcEntry.KeyType != newEntry.KeyType || kit.CryptoKitID != newEntry.CryptoKitId {
                return nil, plan.Error(nil, plan.KeyGenerationFailed, "generate key altered key type")
            }

            krDst.Keys[i] = newEntry

            srcEntry.CryptoKitId = newEntry.CryptoKitId
            srcEntry.TimeCreated = newEntry.TimeCreated
            srcEntry.PubKey      = newEntry.PubKey
            srcEntry.PrivKey     = nil
        }
    }

    return newTome, nil
}



// EqualTo compares if two key entries are identical/interchangable
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
	return entry.KeyType != other.KeyType ||
		entry.CryptoKitId != other.CryptoKitId ||
		entry.TimeCreated != other.TimeCreated ||
		bytes.Equal(entry.PrivKey, other.PrivKey) == false ||
		bytes.Equal(entry.PubKey, other.PubKey) == false

}

// GetKeyID returns the KeyID for this KeyEntry
func (entry *KeyEntry) GetKeyID() plan.KeyID {
	return plan.GetKeyID(entry.PubKey)
}

// ZeroOut zeros out this entry's private key buffer
func (entry *KeyEntry) ZeroOut() {
    N := int32(len(entry.PrivKey))
    for i := int32(0); i < N; i++ {
        entry.PrivKey[i] = 0
    }
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

        case 0, HashKitID_LegacyKeccak_256:
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



// GenerateNewKeys is a convenience bulk function for CryptoKit.GenerateNewKey()
func GenerateNewKeys(
    inRand io.Reader,
    inRequestedKeyLen int,
    inKeySpecs []*KeyEntry,
) ([]*KeyEntry, error) {

    N :=  len(inKeySpecs)

    newKeys := make([]*KeyEntry, N)

    var kit *CryptoKit 
    var err error

    timeCreated := plan.Now().UnixSecs

    for i, keySpec := range inKeySpecs {

        if kit == nil || kit.CryptoKitID != keySpec.CryptoKitId {
            kit, err = GetCryptoKit(keySpec.CryptoKitId)
            if err != nil {
                return nil, err
            }
        }

        newKey := &KeyEntry{
            KeyType: keySpec.KeyType,
            CryptoKitId: kit.CryptoKitID,
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


/*
// GenerateKeys is a convenience function that generates and returns keys via an open SKI session 
func GenerateKeys(
    skiSession Session,
    inCommunityID []byte,
    inKeySpecs []*PubKey,
) ([]*KeyEntry, error) {

    results, err := skiSession.DoOp( OpArgs{
        OpName: OpGenerateKeys,
        CommunityID: inCommunityID,
        KeySpecs: inKeySpecs,
    })

    var newKeys []*KeyEntry

    if err == nil {
        bundleBuf := results.GetContentWithCodec(KeyBundleProtobufCodec, 0)
        keyBundle := KeyBundle{}
        err = keyBundle.Unmarshal(bundleBuf)   
        if err != nil {
            err = plan.Error(err, plan.FailedToUnmarshal, "failed to unmarshal KeyBundle from OpGenerateKeys")
        } else {
            newKeys = keyBundle.Keys

            N := len(keyBundle.Keys)
            plan.Assert(N == len(inKeySpecs), "number of keys returned from GenerateKeys() does not match input")

            for i := 0; i < N; i++ {
                plan.Assert(keyBundle.Keys[i].KeyType == inKeySpecs[i].KeyType, "keys generated from GenerateKeys() don't match request")
            }
        }
    }

    if err != nil {
        return nil, err
    }

    return newKeys, nil

}
*/



// SessionTool is a small set of util functions for creating a SKI session.
type SessionTool struct {
    UserName     string
    Session      Session
    CryptoKitID  CryptoKitID
 //   CommunityID  []byte
    CommunityKey KeyRef
    encryptPubKey   KeyRef

}





// NewSessionTool creates a new tool for helping manage a SKI session.
func NewSessionTool(
    inProvider Provider,
    inUserID string,
    inCommunityID []byte,   // if len()==0, it will be auto-generated
) (*SessionTool, error) {


    st := &SessionTool{
        UserName: inUserID,
        CommunityKey: KeyRef{
            KeyringName: inCommunityID,
        },
    }

/*
    if len(st.CommunityID) == 0 {
        st.CommunityID = make([]byte, plan.CommunityIDSz)
        rand.Read(st.CommunityID)
    }
*/

    path, err := plan.UseLocalDir(inUserID)
    if err != nil {
        return nil, err
    }
    
    st.Session, err = inProvider.StartSession(SessionParams{
        Invocation: plan.Block{
            Label: inProvider.InvocationStr(),
        },
        BaseDir: path,
    })

    return st, err
}



// DoOp performs the given op, blocking until completion
func (st *SessionTool) DoOp(inArgs CryptOpArgs) ([]byte, error) {

    out, err :=  st.Session.DoCryptOp(&inArgs)
    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}



// GenerateNewKey creates a new key, blocking until completion
func (st *SessionTool) GenerateNewKey(
    inKeyType KeyType,
    inKeyringName []byte,
) (*KeyRef, error) {

    out, err := st.Session.DoCryptOp(&CryptOpArgs{
        CryptOp: CryptOp_GENERATE_KEYS,
        TomeIn: &KeyTome {
            Keyrings: []*Keyring{
                &Keyring{
                    Name: inKeyringName,
                    Keys: []*KeyEntry{
                        &KeyEntry{
                            KeyType: inKeyType,
                            CryptoKitId: st.CryptoKitID,
                        },
                    },
                },
            },
        },
    })

    var kr *Keyring
    if err == nil && out != nil && out.TomeOut != nil && out.TomeOut.Keyrings[0] != nil {
        kr = out.TomeOut.Keyrings[0]
    }

    if kr == nil || kr.Keys[0] == nil {
        return nil, plan.Error(nil, plan.AssertFailed, "no keys returned")
    }

    if kr.Keys[0].KeyType     != inKeyType || 
       kr.Keys[0].CryptoKitId != st.CryptoKitID {

        return nil, plan.Error(nil, plan.AssertFailed, "unexpected key type")
    }

    return &KeyRef{
        KeyringName: kr.Name,
        PubKey: kr.Keys[0].PubKey,
    }, nil
}

// EndSession ends the current session
func (st *SessionTool) EndSession(inReason string) {

    st.Session.EndSession(inReason)

}



// FormKeyringName forms a base 64 encoded representation of inPrefix concatenated with inSuffix
func FormKeyringName(
    inPrefix []byte,
    inSuffix []byte,
) string {

    mid := len(inPrefix)
    binSz := mid + len(inSuffix)

    strSz := plan.Base64.EncodedLen(binSz)
    bin := make([]byte, binSz + strSz)
    copy(bin[:mid], inPrefix)
    copy(bin[mid:binSz], inSuffix)

    str := bin[binSz:]
    plan.Base64.Encode(str, bin)

	return string(str)  
}



/*

// KeyringNameForCommunity makes a string from the given community ID, intended for use community-public scoped use.
// Returns "" if the community ID is invalid.
func KeyringNameForCommunity(
    inCommunityID []byte,
) string {

    const (
        keySetCommSz   = 18
    )

    if len(inCommunityID) < keySetCommSz {
        return ""
    }

    return FormKeyringName(inCommunityID[:keySetCommSz], nil)
}


func KeyringNameForStorage(
    inCommunityID []byte,
    inStorageID plan.StorageID,
) string {

    var storageID [2]byte

    storageID[0] = byte(inStorageID)
    storageID[1] = byte(inStorageID >> 8)
    
    return FormKeyringName(inCommunityID[:keySetCommSz], nil)

}



// KeyringNameForMember makes a string composite from the given parameters, used for name/index a ski.Keyring.
// Returns "" if the community ID is invalid.
func KeyringNameForMember(
    inCommID []byte,
    inMemberID plan.MemberID,
) string {

    const (
        keySetCommSz      = 16
        keySetMemberSz    =  8
    )

    if len(inCommID) < keySetCommSz {
        return ""
    }

    var mem [keySetMemberSz]byte
    for i := 0; i < keySetMemberSz; i++ {
        mem[i] = byte(inMemberID)
        inMemberID >>= 8
    }

    return FormKeyringName(inCommunityID[:keySetCommSz], mem[:]) 
}


*/