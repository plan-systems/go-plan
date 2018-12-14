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

// FetchKeyringSet returns the KeyringSet associated with the given community ID 
func (KR *KeyRepo) FetchKeyringSet(
    inCommunityID []byte,
) (*KeyringSet, *plan.Perror) {

    CID := plan.GetCommunityID(inCommunityID)
 
    KR.RLock()
    keyringSet, ok := KR.ByCommunity[CID]
    KR.RUnlock()

    if ! ok {
        return nil, plan.Errorf(nil, plan.KeyringNotFound, "no keyrings found for community ID %v", inCommunityID)
    }

    return keyringSet, nil
}





// intenal struct that holds KeyEntries by KeyID.
type keyring struct {
    KeysByID        map[plan.KeyID]*KeyEntry
}


// KeyringSet organizes a set of KeyEntry by ski.KeyDomain.
type KeyringSet struct {
    sync.RWMutex

    ByKeyDomain     [NumKeyDomains]keyring
}






// GenerateNewKeys generates the requested keys and adds them to this KeyringSet
func (ks *KeyringSet) GenerateNewKeys(
    ioKeyReqs []*KeyEntry,
) *plan.Perror {

    var err *plan.Perror
    var newKeys []*KeyEntry

    for {

        newKeys, err = GenerateNewKeys(crypto_rand.Reader, 32, ioKeyReqs)
        if err != nil {
            return err
        }

        // Let's all laugh and be merry at a 1:2^256 collision!  I want a pony and air-jammer-road-rammer!
        // But seriously ladies and gentleman, we just loop if it happens.  
        keysNotMerged := ks.ImportKeys(newKeys)
        if len(keysNotMerged) == 0 {
            break
        }
    }

    if err == nil {
        plan.Assert(len(newKeys) == len(ioKeyReqs), "GenerateNewKeys() key count mismatch")

        for i, newEntry := range newKeys {
            req := ioKeyReqs[i]
            plan.Assert( 
                req.KeyType == newEntry.KeyType &&
                req.KeyDomain == newEntry.KeyDomain &&
                (req.CryptoKitId == CryptoKitID_DEFAULT_KIT_ID || req.CryptoKitId == newEntry.CryptoKitId),
                "GenerateNewKeys() key param check failed")

            req.CryptoKitId = newEntry.CryptoKitId
            req.TimeCreated = newEntry.TimeCreated
            req.PubKey = newEntry.PubKey
            req.PrivKey = nil
        }
    }

    return err
}




func (ks *KeyringSet) getKeyEntryInternal(
    inKeySpec *KeyEntry,
) (*KeyEntry, *plan.Perror) {

    var err *plan.Perror
    var keyEntry *KeyEntry

    if inKeySpec.KeyDomain < 0 || inKeySpec.KeyDomain > NumKeyDomains {
        err = plan.Errorf(nil, plan.KeyDomainNotFound, "key domain not found {KeyDomain: %v}", inKeySpec.KeyDomain)
    }
    
    if err == nil {
        keyID := plan.GetKeyID(inKeySpec.PubKey)
        keyEntry = ks.ByKeyDomain[inKeySpec.KeyDomain].KeysByID[keyID]
    }

    if keyEntry == nil && err == nil {
        err = plan.Errorf(nil, plan.KeyEntryNotFound, "key not found {PubKey:%v}", inKeySpec.PubKey)
    }
   
    return keyEntry, err
}



// FetchKeys fetches the KeyEntry for each element in inKeySpecs (by .KeyDomain and .PubKey).  
// If a key spec IS found, the full KeyEntry ptr is appended to ioKeys.  TREAT AS READ ONLY, ESP SINCE IT CONTAINS THE PRIVATE KEY.
// If a key spec is NOT found, the requested key spec is appended to a slice and returned.  i.e. if all keys were found, the return value is nil.
func (ks *KeyringSet) FetchKeys(
    inKeySpecs []*KeyEntry,
    ioKeyBundle *KeyBundle,
)  []*KeyEntry {

    var keysNotFound []*KeyEntry

    keysFound := make([]*KeyEntry, 0, len(inKeySpecs))

    ks.RLock()
    {
        for _, keySpec := range inKeySpecs {
            keyEntry, _ := ks.getKeyEntryInternal(keySpec)
            
            if keyEntry != nil {
                keysFound = append(keysFound, keyEntry)
            } else {
                keysNotFound = append(keysNotFound, keySpec)
            }
        }
    }
    ks.RUnlock()

    ioKeyBundle.Keys = append(ioKeyBundle.Keys, keysFound...)

    return keysNotFound
}

// FetchKey is identical to FetchNamedKeys() except is for only one key.
func (ks *KeyringSet) FetchKey(
    inKeySpec *KeyEntry,
) (*KeyEntry, *plan.Perror) {

    ks.RLock()
    keyEntry, err := ks.getKeyEntryInternal(inKeySpec)
    ks.RUnlock()

    return keyEntry, err
}



// GetKeyring returns an entire KeyDomain
func (ks *KeyringSet) GetKeyring(
    inKeyDomain KeyDomain,
) ([]*KeyEntry, *plan.Perror) {

    if inKeyDomain < 0 || inKeyDomain > NumKeyDomains {
        return nil, plan.Errorf(nil, plan.KeyDomainNotFound, "keyring not found {KeyDomain: %v}", inKeyDomain)
    }

    ks.RLock()
    
    keysByID := ks.ByKeyDomain[inKeyDomain].KeysByID
    outKeys := make([]*KeyEntry, 0, len(keysByID))
    for _, keyEntry := range keysByID {
        outKeys = append(outKeys, keyEntry)
    }

    ks.RUnlock()

    // TODO: keys sort by time or by ID

   return outKeys, nil
}

/*
func SerializeKeysAsBundle(
    inKeys []*KeyEntry,
) ([]byte, *plan.Perror) {

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
func (ks *KeyringSet) ImportKeys(
    inKeysToMerge []*KeyEntry,
) []*KeyEntry {

    var keysSkipped []*KeyEntry
        
    ks.Lock()
    for _, keyEntry := range inKeysToMerge {

        if keyEntry != nil {
            merged := false
            if keyEntry.KeyDomain >= 0 && keyEntry.KeyDomain < NumKeyDomains {
                keyID := keyEntry.GetKeyID()
                keyring := &ks.ByKeyDomain[keyEntry.KeyDomain]
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
    ks.Unlock()

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
    ) *plan.Perror {

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

    var err *plan.Perror

    if len(collisions) > 0 {
        err = plan.Errorf(nil, plan.KeyIDCollision, "key ID collision while adding keys {keyID:%v}", collisions)
    }
    return err

}
*/
