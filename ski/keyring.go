package ski // import "github.com/plan-tools/go-plan/ski"

import (
	crypto_rand "crypto/rand"
    "sync"
    "bytes"

	plan "github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	sign "golang.org/x/crypto/nacl/sign"
)




// Keyring contains a set of KeyEntry
type Keyring struct {
    sync.RWMutex
    
    Label           string
    KeysCodec       string
    keysByID        map[plan.KeyID]*KeyEntry
}

// NewKeyring creates and empty keyring with the given label/name.
func NewKeyring(inLabel string) *Keyring {

    return &Keyring{
        Label: inLabel,
        KeysCodec: NaClKeysCodec,
        keysByID: map[plan.KeyID]*KeyEntry{},
    }
}



// NewIdentity generates encryption and signing keys, adds them to the
// Keyring, and returns the public keys associated with those private
// keys.
func (kr *Keyring) NewIdentity() (outSigningKey plan.IdentityPublicKey, outEncKey plan.IdentityPublicKey) {

    encrKey := generateKeyEntry(naclEncryptionKey)
    signKey := generateKeyEntry(naclSigningKey)

	// store it in the Keyring and return the public keys
    kr.Lock()
	kr.keysByID[encrKey.GetKeyID()] = encrKey
    kr.keysByID[signKey.GetKeyID()] = signKey
    kr.Unlock()

	return signKey.PubKey, encrKey.PubKey
}



// NewSymmetricKey generates a new symmetric key and adds it to the Keyring,
// and returns the CommunityKeyID associated with that key.
func (kr *Keyring) NewSymmetricKey() plan.KeyID {
	
    symKey := generateKeyEntry(naclSymmetricKey)

    keyID := symKey.GetKeyID()

    kr.Lock()
    kr.keysByID[keyID] = symKey
    kr.Unlock()

	return keyID
}



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



// GetSigningKey fetches the d's private signing key from the keychain for a
// specific public key, or an error if the key doesn't exist.
func (kr *Keyring) GetSigningKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {

	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()
    
	if !ok || entry.KeyInfo != naclSigningKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "signing key not found {keyID:%v}", inKeyID)
	}
	return entry.PrivKey, nil
}



// GetEncryptKey fetches the d's private encrypt key from the keychain,
// or an error if the key doesn't exist.
func (kr *Keyring) GetEncryptKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {
    
	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()

	if !ok || entry.KeyInfo != naclEncryptionKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "encrypt key not found {keyID:%v}", inKeyID)
	}
	return entry.PrivKey, nil
}



// GetSymmetricKey fetches the community key from the keychain for a
// based on its ID, or an error if the key doesn't exist.
func (kr *Keyring) GetSymmetricKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {
    
	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()

	if !ok || entry.KeyInfo != naclSymmetricKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "community key not found {keyID:%v}", inKeyID)
	}
	return entry.PrivKey, nil
}





func generateKeyEntry(inKeyInfo uint32) *KeyEntry {
    entry := &KeyEntry{
        KeyInfo: inKeyInfo,
        TimeCreated: plan.Now().UnixSecs,
    }
    
    switch inKeyInfo {

        case naclSymmetricKey:{
            entry.PubKey = make([]byte, plan.KeyIDSz)
            _, err := crypto_rand.Read(entry.PubKey)
            if err != nil {
                panic(err)
            }

            entry.PrivKey = make([]byte, 32)
            _, err = crypto_rand.Read(entry.PrivKey)
            if err != nil {
                panic(err)
            }
        }

        case naclEncryptionKey:{
            publicKey, privateKey, err := box.GenerateKey(crypto_rand.Reader)
            if err != nil {
                panic(err)
            }
            entry.PrivKey = privateKey[:]
            entry.PubKey = publicKey[:]
        }

        case naclSigningKey:{
            publicKey, privateKey, err := sign.GenerateKey(crypto_rand.Reader)
            if err != nil {
                panic(err)
            }
            entry.PrivKey = privateKey[:]
            entry.PubKey = publicKey[:]
        }
    }

    return entry

}







// EqualTo compares if two key entries are identical/interchangable 
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
    return entry.KeyInfo != other.KeyInfo ||
        entry.TimeCreated != other.TimeCreated ||
        bytes.Equal(entry.PrivKey, other.PrivKey) == false ||
        bytes.Equal(entry.PubKey, other.PubKey) == false

}

// GetKeyID is a convenience function that returns this key's fixed ID (determined by the key's public key)
func (entry *KeyEntry) GetKeyID() plan.KeyID {
    var keyID plan.KeyID
    copy(keyID[:], entry.PubKey[len(entry.PubKey)-plan.KeyIDSz:])
    return keyID
}





