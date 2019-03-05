package ski

import (
    "sync"
	crypto_rand "crypto/rand"

	"github.com/plan-systems/go-plan/plan"

)

// KeyRepo is a memory-resident data structure that stores KeyEntry, placed in a hierarchy of data structures, indexed by:
//    CommunityID => KeyDomain => KeyEntry.PubKey
type KeyRepo struct {
    sync.RWMutex

    ByCommunity   map[plan.CommunityID]*KeyringSet
}


// NewKeyRepo creates a newly initialized KeyRepo
func NewKeyRepo() *KeyRepo {
    return &KeyRepo{
        ByCommunity: map[plan.CommunityID]*KeyringSet{},
    }
}

// Clear resets the given KeyRepo as if NewKeyRepo() was called instead.
func (KR *KeyRepo) Clear() {

    // TODO: zero out each key entry to protect private keys
    KR.RLock()
    for k := range KR.ByCommunity {
        delete(KR.ByCommunity, k)
    }
    KR.RUnlock()

}


// FetchKeyringSet returns the KeyringSet associated with the given community ID 
func (KR *KeyRepo) FetchKeyringSet(
    inCommunityID []byte,
    inAutoCreate bool,
) (*KeyringSet, error) {

    CID := plan.GetCommunityID(inCommunityID)
 
    KR.RLock()
    krSet, ok := KR.ByCommunity[CID]
    if ! ok && inAutoCreate {
        krSet = &KeyringSet{
            CommunityID: inCommunityID,
        }
        for i := 0; i < int(NumKeyDomains); i++ {
            krSet.ByKeyDomain[i] = keyring{
                map[plan.KeyID]*KeyEntry{},
            }
        }
        KR.ByCommunity[CID] = krSet
        ok = true
    }
    KR.RUnlock()

    if ! ok {
        return nil, plan.Errorf(nil, plan.KeyringNotFound, "no keyrings found for community ID %v", inCommunityID)
    }

    return krSet, nil
}




// Marshal writes out entire state to a given buffer.
// Warning: the return buffer is not encrypted and contains private key data!
func (KR *KeyRepo) Marshal() ([]byte, error) {

    KR.RLock()

    keyTome := KeyTome {
        TimeCreated: plan.Now().UnixSecs,
        Bundles: make([]*KeyBundle, 0, len(KR.ByCommunity)),
    }

    for _, krSet := range KR.ByCommunity {

        krSet.RLock()
        {
            entryCount := 0

            for i := 0; i < int(NumKeyDomains); i++ {
                entryCount += len(krSet.ByKeyDomain[i].KeysByID)
            }

            bundle := &KeyBundle{
                CommunityId: krSet.CommunityID,
                Keys:        make([]*KeyEntry, 0, entryCount),
            }

            for i := 0; i < int(NumKeyDomains); i++ {
                for _, keyEntry := range krSet.ByKeyDomain[i].KeysByID {
                    bundle.Keys = append(bundle.Keys, keyEntry)
                }
            }

            keyTome.Bundles = append(keyTome.Bundles, bundle)
        }
        krSet.RUnlock()
    }

    dAtA, err := keyTome.Marshal()
    if err != nil {
        return nil, plan.Errorf(err, plan.FailedToMarshal, "Unexpected error marshaing KeyTome")
    }

    return dAtA, nil
}


// Unmarshal resets this KeyRepo from the state data written out by Marshal()
func (KR *KeyRepo) Unmarshal(dAtA []byte) error {

    keyTome := KeyTome{}

    if err := keyTome.Unmarshal(dAtA); err != nil {
        return plan.Errorf(err, plan.FailedToUnmarshal, "Unexpected error unmarshalling KeyTome")
    }

    for _, bundle := range keyTome.Bundles {
        krSet, _ := KR.FetchKeyringSet(bundle.CommunityId, true)
        krSet.ImportKeys(bundle.Keys)
    }

    return nil
}





// intenal struct that holds KeyEntries by KeyID.
type keyring struct {
    KeysByID        map[plan.KeyID]*KeyEntry
}


// KeyringSet organizes a set of KeyEntry by ski.KeyDomain.
type KeyringSet struct {
   
    sync.RWMutex
    
    CommunityID     []byte
    ByKeyDomain     [NumKeyDomains]keyring 
}





// GenerateNewKeys generates the requested keys and adds them to this KeyringSet
func (krSet *KeyringSet) GenerateNewKeys(
    inKeySpecs []*PubKey,
) ([]*KeyEntry, error) {

    var err error
    var newKeys []*KeyEntry

    for {

        newKeys, err = GenerateNewKeys(crypto_rand.Reader, 32, inKeySpecs)
        if err != nil {
            return nil, err
        }

        // Let's all laugh and be merry at a 1:2^256 collision!  I want a pony and air-jammer-road-rammer!
        // But seriously ladies and gentleman, we just loop if it happens.  
        keysNotMerged := krSet.ImportKeys(newKeys)
        if len(keysNotMerged) == 0 {
            break
        }
    }

    if err == nil {
        plan.Assert(len(newKeys) == len(inKeySpecs), "GenerateNewKeys() key count mismatch")

        for i, entry := range newKeys {
            spec := inKeySpecs[i]

            // Export a dupe, sans private key
            newKeys[i] = &KeyEntry{
                KeyType:     entry.KeyType,
                KeyDomain:   entry.KeyDomain,
                CryptoKitId: entry.CryptoKitId,
                TimeCreated: entry.TimeCreated,
                PubKey:      entry.PubKey,
            }

            plan.Assert( 
                spec.KeyType == entry.KeyType &&
                spec.KeyDomain == entry.KeyDomain &&
                (spec.CryptoKitId == CryptoKitID_DEFAULT_KIT || spec.CryptoKitId == entry.CryptoKitId),
                "GenerateNewKeys() key param check failed")

        }
    }

    return newKeys, err
}




func (krSet *KeyringSet) getKeyEntryInternal(
    inKeySpec *PubKey,
) (*KeyEntry, error) {

    var err error
    var keyEntry *KeyEntry

    if inKeySpec.KeyDomain < 0 || inKeySpec.KeyDomain > NumKeyDomains {
        err = plan.Errorf(nil, plan.KeyDomainNotFound, "key domain not found {KeyDomain: %v}", inKeySpec.KeyDomain)
    }
    
    if err == nil {
        keyID := plan.GetKeyID(inKeySpec.Bytes)
        keyEntry = krSet.ByKeyDomain[inKeySpec.KeyDomain].KeysByID[keyID]
    }

    if keyEntry == nil && err == nil {
        err = plan.Errorf(nil, plan.KeyEntryNotFound, "key not found {PubKey:%v}", inKeySpec.Bytes)
    }
   
    return keyEntry, err
}



// FetchKeys fetches the KeyEntry for each element in inKeySpecs (by .KeyDomain and .PubKey).  
// If a key spec IS found, the full KeyEntry ptr is appended to ioKeys.  TREAT AS READ ONLY, ESP SINCE IT CONTAINS THE PRIVATE KEY.
// If a key spec is NOT found, the requested key spec is appended to a slice and returned.  i.e. if all keys were found, the return value is nil.
func (krSet *KeyringSet) FetchKeys(
    inKeySpecs []*PubKey,
    ioKeyBundle *KeyBundle,
) []*PubKey {

    var keysNotFound []*PubKey

    keysFound := make([]*KeyEntry, 0, len(inKeySpecs))

    krSet.RLock()
    {
        for _, keySpec := range inKeySpecs {
            keyEntry, _ := krSet.getKeyEntryInternal(keySpec)
            
            if keyEntry != nil {
                keysFound = append(keysFound, keyEntry)
            } else {
                keysNotFound = append(keysNotFound, keySpec)
            }
        }
    }
    krSet.RUnlock()

    ioKeyBundle.Keys = append(ioKeyBundle.Keys, keysFound...)

    return keysNotFound
}

// FetchKey is identical to FetchNamedKeys() except is for only one key.
func (krSet *KeyringSet) FetchKey(
    inKeySpec *PubKey,
) (*KeyEntry, error) {

    krSet.RLock()
    keyEntry, err := krSet.getKeyEntryInternal(inKeySpec)
    krSet.RUnlock()

    return keyEntry, err
}



// GetKeyring returns an entire KeyDomain
func (krSet *KeyringSet) GetKeyring(
    inKeyDomain KeyDomain,
) ([]*KeyEntry, error) {

    if inKeyDomain < 0 || inKeyDomain > NumKeyDomains {
        return nil, plan.Errorf(nil, plan.KeyDomainNotFound, "keyring not found {KeyDomain: %v}", inKeyDomain)
    }

    krSet.RLock()
    
    keysByID := krSet.ByKeyDomain[inKeyDomain].KeysByID
    outKeys := make([]*KeyEntry, 0, len(keysByID))
    for _, keyEntry := range keysByID {
        outKeys = append(outKeys, keyEntry)
    }

    krSet.RUnlock()

    // TODO: keys sort by time or by ID

   return outKeys, nil
}



/*
func SerializeKeysAsBundle(
    inKeys []*KeyEntry,
) ([]byte, error) {

    bundle := KeyBundle{
        Keys: inKeys
    }
}


    keys, err := ks.ExportKeyring(inKeyDomain)
    if err != nil {
        return nil, err
    }
    if inKeyDomain < 0 || inKeyDomain > NumKeyDomains {
        return nil, plan.Errorf(nil, plan.KeyDomainNotFound, "keyring not found {KeyDomain: %v}", inKeyDomain)
    }

    ks.RLock()
    
    keysByID := ks.ByKeyDomain[inKeyDomain].KeysByID
    outKeys := make([]*KeyEntry, len(keysByID), 0)
    for _, keyEntry := range keysByID {
        outKeys = append(outKeys, keyEntry)
    }

    ks.RUnlock()

    // TODO: keys sort by time

   return outKeys, nil
}
*/



// ImportKeys adds the keys to this KeyringSet returning a list of the collisions not merged.
// If an incoming collides (and isn't exactly identical to the existing entry), 
//    then it is added to list of keys returned (i.e. this func merges the keys it can).
func (krSet *KeyringSet) ImportKeys(
    inKeysToMerge []*KeyEntry,
) []*KeyEntry {

    var keysSkipped []*KeyEntry
        
    krSet.Lock()
    for _, keyEntry := range inKeysToMerge {

        if keyEntry != nil {
            merged := false
            if keyEntry.KeyDomain >= 0 && keyEntry.KeyDomain < NumKeyDomains {
                keyID := keyEntry.GetKeyID()
                keyring := &krSet.ByKeyDomain[keyEntry.KeyDomain]
                existing := keyring.KeysByID[keyID]
                if existing == nil || existing.EqualTo(keyEntry) {
                    merged = true
                    if existing == nil {
                        keyring.KeysByID[keyID] = keyEntry
                    }
                }
            }
            
            if ! merged {
                keysSkipped = append(keysSkipped, keyEntry)
            }
        }
    }
    krSet.Unlock()

    return keysSkipped

}


/*

// ExportKeys exports the given list of keys into a buffer t
func (kr *Keyring) ExportKeys(
    inKeyIDs []plan.KeyID,
    ioKeyList *KeyList,
    ) []plan.KeyID {

    var keysNotFound []plan.KeyID
    
    kr.RLock()
    for _, keyID := range inKeyIDs {
        existing := kr.keysByID[keyID]
        if existing != nil {
            ioKeyList.Keys = append(ioKeyList.Keys, existing)
        } else {
            keysNotFound = append(keysNotFound, keyID)
        }
    }
    kr.RUnlock()

    return keysNotFound

}


// MergeKeys adds a key to the keychain (ignoring collitions if the key entry is identical)
func (kr *Keyring) MergeKeys(
    inKeyList KeyList,
    ) error {

    var collisions []*KeyEntry
    var keyID plan.KeyID
    
    kr.Lock()
    for _, entry := range inKeyList.Keys {
        keyID = entry.GetKeyID()
        existing := kr.keysByID[keyID]
        if existing != nil && ! existing.EqualTo(entry) {
            collisions = append(collisions, entry)
        } else {
            kr.keysByID[keyID] = entry
        }
    }
    kr.Unlock()

    var err error

    if len(collisions) > 0 {
        err = plan.Errorf(nil, plan.KeyIDCollision, "key ID collision while adding keys {keyID:%v}", collisions)
    }
    return err

}
*/
