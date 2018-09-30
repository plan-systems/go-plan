package file

import (
    "sync"

	"github.com/plan-tools/go-plan/ski"
	"github.com/plan-tools/go-plan/plan"

)




// Keyring contains a set of KeyEntry
type Keyring struct {
    KeysByID        map[plan.KeyID]*ski.KeyEntry
}



type Keyrings struct {
    sync.RWMutex

    ByKeyDomain     [ski.NumKeyDomains]Keyring
}



func (keyrings *Keyrings) GenerateNewKeys(
    inKeySpecs ski.KeySpecs,
) ([]*ski.KeyEntry, *plan.Perror) {


}



func (keyrings *Keyrings) ExportNamedKeys(
    inKeySpecs ski.KeySpecs,
    inErrorOnKeyNotFound bool,
) ([]*ski.KeyEntry, *plan.Perror) {

    outKeys := make([]*ski.KeyEntry, len(inKeySpecs.Specs))

    keyrings.RLock()
    defer keyrings.RUnlock()

   for i, keySpec := range inKeySpecs.Specs {

        var err plan.Perror
        var keyEntry *ski.KeyEntry

        if keySpec.KeyDomain < 0 || keySpec.KeyDomain > ski.NumKeyDomains {
            err = plan.Errorf(nil, plan.KeyDomainNotFound, "key domain not found {%v}", keySpec.KeyDomain)
        }
        
        if err == nil {
            keyID := plan.GetKeyID(keySpec.PubKey)

            outKeys[i] = keyrings.ByKeyDomain[keySpec.KeyDomain].KeysByID[keyID]
            if keyEntry == nil {
                err = plan.Errorf(nil, plan.KeyIDNotFound, "one or more keys not found {keyID:%v}", inKeyID)
            }
        }

        // If we don't error out, nil is left in elements, allowing the calling to know what keys weren't found
        if err != nil && inErrorOnKeyNotFound {
            return nil, err
        }
   }

   return outKeys, nil
}


func (keyrings *Keyrings) ExportKeyring(
    inKeyDomain ski.KeyDomain,
) ([]*ski.KeyEntry) {


    keyrings.RLock()

    keyring := keyrings.ByKeyDomain[keySpec.KeyDomain]
    
    outKeys := make([]*ski.KeyEntry, len(keyring))

    for i, keyEntry := range keyring.KeysByID {
        outKeys[i] = keyEntry
    }

    keyrings.RUnlock()

    // TODO: sort by time

   return outKeys
}



type KeyRepo struct {
    sync.RWMutex

    ByCommunity   map[plan.CommunityID]*Keyrings
}


func (keyRepo *KeyRepo) FetchKeyrings(
    inCommunityID []byte,
) (*Keyrings, *plan.Perror) {

    CID := plan.GetCommunityID(inCommunityID)

    keyRepo.RLock()
    keyrings, ok := keyRepo.ByCommunity[CID]
    keyRepo.RUnlock()

    if ! ok {
        return nil, plan.Errorf(nil, plan.KeyringNotFound, "no keyrings found for community ID %v", inCommunityID)
    }

    return keyrings, nil
}





// NewKeyring creates and empty keyring with the given label/name.
func NewKeyring(inLabel string) *Keyring {

    return &Keyring{
        Label: inLabel,
        KeysCodec: KeysCodec,
        keysByID: map[plan.KeyID]*ski.KeyEntry{},
    }
}


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


// GetKey fetches the key from the keychain,
// or an error if the key doesn't exist.
func (kr *Keyring) GetKey(inKeyID plan.KeyID) (
	*ski.KeyEntry, *plan.Perror) {
    
	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()

	if !ok {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "key not found {keyID:%v}", inKeyID)
	}
	return entry, nil
}



