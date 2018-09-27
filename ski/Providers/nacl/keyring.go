package nacl

import (
	crypto_rand "crypto/rand"
    "sync"

	"github.com/plan-tools/go-plan/ski"

	plan "github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	sign "golang.org/x/crypto/nacl/sign"
)




// Keyring contains a set of KeyEntry
type Keyring struct {
    sync.RWMutex
    
    Label           string
    KeysCodec       string
    keysByID        map[plan.KeyID]*ski.KeyEntry
}

// NewKeyring creates and empty keyring with the given label/name.
func NewKeyring(inLabel string) *Keyring {

    return &Keyring{
        Label: inLabel,
        KeysCodec: KeysCodec,
        keysByID: map[plan.KeyID]*ski.KeyEntry{},
    }
}



// NewIdentity generates encryption and signing keys, adds them to the
// Keyring, and returns the public keys associated with those private keys.
func (kr *Keyring) NewIdentity() (outSigningKey []byte, outEncKey []byte) {

    encrKey := kr.NewKeyEntry(naclEncryptionKey)
    signKey := kr.NewKeyEntry(naclSigningKey)

    return signKey.PubKey, encrKey.PubKey
}



// NewSymmetricKey generates a new symmetric key and adds it to the Keyring,
// and returns the CommunityKeyID associated with that key.
func (kr *Keyring) NewSymmetricKey() plan.KeyID {
    return kr.NewKeyEntry(naclSymmetricKey).GetKeyID()
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





func generateKeyEntry(inKeyInfo uint32) *ski.KeyEntry {
    entry := &ski.KeyEntry{
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




