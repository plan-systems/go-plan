package file

import (
    "sync"
	crypto_rand "crypto/rand"

	"github.com/plan-tools/go-plan/ski"
	"github.com/plan-tools/go-plan/plan"

)




// Keyring contains a set of KeyEntry
type keyring struct {
    KeysByID        map[plan.KeyID]*ski.KeyEntry
}



type KeyringSet struct {
    sync.RWMutex

    ByKeyDomain     [ski.NumKeyDomains]keyring
}



func (set *KeyringSet) GenerateNewKeys(
    inKeyReqs []*KeyEntry,
) ([]*ski.KeyEntry, *plan.Perror) {

    keysNotMerged := inKeyReqs

    // Loop and hope we don't have a ~ 2^256 collusion.  Good luck!
    for ; len(keysNotMerged) > 0; {

        newKeys, err := ski.GenerateNewKeys(crypto_rand.Reader, 32, keysNotMerged)
        if err != nil {
            return err
        }

        // Let's all laugh a give cheers to the possibility of a 1:2^256 collision!
        keysNotMerged := set.MergeKeys(newKeys)
        if len(keysNotMerged) == 0 {
            break
        }
    }
   
}


// ExportNamedKeys returns a list of KeyEntries, where each element corresponds to the elements in inKeySpecs. 
// If inErrorOnKeyNotFound is set and a KeySpec isn't found, an error is returned.  Otherwise, nil is returned for that element.
func (set *KeyringSet) ExportNamedKeys(
    inKeySpecs ski.KeySpecs,
    inErrorOnKeyNotFound bool,
) ([]*ski.KeyEntry, *plan.Perror) {

    outKeys := make([]*ski.KeyEntry, len(inKeySpecs.Specs))

    set.RLock()
    defer set.RUnlock()

    for i, keySpec := range inKeySpecs.Specs {

        var err plan.Perror
        var keyEntry *ski.KeyEntry

        if keySpec.KeyDomain < 0 || keySpec.KeyDomain > ski.NumKeyDomains {
            err = plan.Errorf(nil, plan.KeyDomainNotFound, "key domain not found {KeyDomain: %v}", keySpec.KeyDomain)
        }
        
        if err == nil {
            keyID := plan.GetKeyID(keySpec.PubKey)

            keyEntry = set.ByKeyDomain[keySpec.KeyDomain].KeysByID[keyID]
            if inErrorOnKeyNotFound && keyEntry == nil {
                err = plan.Errorf(nil, plan.KeyIDNotFound, "one or more keys not found {keyID:%v}", inKeyID)
            }

            outKeys[i] = keyEntry
        }

        // If we don't error out, nil is left in elements, allowing the calling to know what keys weren't found
        if err != nil && inErrorOnKeyNotFound {
            return nil, err
        }
   }

   return outKeys, nil
}


func (set *KeyringSet) ExportKeyring(
    inKeyDomain ski.KeyDomain,
) ([]*ski.KeyEntry) {

    if inKeyDomain < 0 || inKeyDomain > ski.NumKeyDomains {
        return plan.Errorf(nil, plan.KeyDomainNotFound, "keyring not found {KeyDomain: %v}", inKeyDomain)
    }

    outKeys := make([]*ski.KeyEntry, len(keyring))

    keyrings.RLock()
    {
        keyring := set.ByKeyDomain[keySpec.KeyDomain]
    
        for i, keyEntry := range keyring.KeysByID {
            outKeys[i] = keyEntry
        }
    }
    keyrings.RUnlock()

    // TODO: keys sort by time

   return outKeys
}




// MergeKeys adds the keys to this KeyringSet returning a list of the collisions not merged.
// If an incoming collides (and isn't exactly identical to the existing entry), 
//    then it is added to list of keys returned (i.e. this func merges the keys it can).
func (set *KeyringSet) MergeKeys(
    inKeysToMerge []*KeyEntry,
) []*ski.KeyEntry {

    var keysNotMerged []*ski.KeyEntry
    var keyID plan.KeyID
    
    var errs []*plan.Perror
    
    set.Lock()
    for _, keyEntry := range inKeysToMerge {

        keyMerged := false
        if keyEntry.KeyDomain >= 0 && keyEntry.KeyDomain < ski.NumKeyDomains {

            keyID := entry.GetKeyID()
            keyring := &set.ByKeyDomain[keySpec.KeyDomain]
            existing := keyring.KeysByID[keyID]
            if existing == nil || existing.EqualTo(entry) {
                keyMerged = true
                if existing == nil {
                    keyring.KeysByID[keyID] = keyEntry
                }
            }
            
        }
        
        if ! keyMerged {
            keysNotMerged = append(keysNotMerged, keyEntry)
        }
    }
    set.Unlock()

/*
    if len(collisions) > 0 {
        err = plan.Errorf(nil, plan.KeyIDCollision, "key ID collision while adding keys {keyID:%v}", collisions)
    }*/

    return keysNotMerged

}


type KeyRepo struct {
    sync.RWMutex

    ByCommunity   map[plan.CommunityID]*KeyringSet
}


func (keyRepo *KeyRepo) FetchKeyrings(
    inCommunityID []byte,
) (*KeyringSet, *plan.Perror) {

    CID := plan.GetCommunityID(inCommunityID)
 
    keyRepo.RLock()
    keyringSet, ok := keyRepo.ByCommunity[CID]
    keyRepo.RUnlock()

    if ! ok {
        return nil, plan.Errorf(nil, plan.KeyringNotFound, "no keyrings found for community ID %v", inCommunityID)
    }

    return keyringSet, nil
}



/*

// NewKeyring creates and empty keyring with the given label/name.
func NewKeyring(inLabel string) *Keyring {

    return &Keyring{
        Label: inLabel,
        KeysCodec: KeysCodec,
        keysByID: map[plan.KeyID]*ski.KeyEntry{},
    }
}
*/

// NewKeyEntry generates a new KeyEntry of the given type 
func (kr *Keyring) NewKeyEntry(inKeyInfo uint32) *ski.KeyEntry {

    for {
        keyEntry := generateKeyEntry(inKeyInfo)

        keyID := keyEntry.GetKeyID()
    
        kr.Lock()

        // If we get a collision, keep regenerating a key
        if kr.keysByID[keyID] != nil {
            keyEntry = nil
        } else {
            kr.keysByID[keyID] = keyEntry            
        }
        kr.Unlock()
    
        if keyEntry != nil {
            return keyEntry
        }
    }
   

}
/*

// ExportKeys exports the given list of keys into a buffer t
func (kr *Keyring) ExportKeys(
    inKeyIDs []plan.KeyID,
    ioKeyList *ski.KeyList,
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
    inKeyList ski.KeyList,
    ) *plan.Perror {

    var collisions []*ski.KeyEntry
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

/*
func (kr *Keyring) GetKeyEntry(
    inKeyID plan.KeyID,
    outKeyEntry *KeyEntry,
    ) *plan.Perror {

    var keyEntry KeyEntry

	kr.RLock()
    keyEntry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()
    
	if !ok {
		return plan.Errorf(nil, plan.KeyIDNotFound, "key not found {keyID:%v}", inKeyID)
    }
    
    *outKeyEntry = keyEntry

	return nil
}
*/