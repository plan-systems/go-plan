package ski // import "github.com/plan-tools/go-plan/ski"

import (
	crypto_rand "crypto/rand"
	"sync"
    "bytes"

	plan "github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	sign "golang.org/x/crypto/nacl/sign"
)


type KeyType uint32
const (
    SymmetricKey        KeyType = 1
    EncryptionKey       KeyType = 2
    SigningKey          KeyType = 3
)

type KeyEntry struct {
    keyID           plan.KeyID                  `json:"id"`
    keyType         KeyType                     `json:"type"`
    creationTime    int64                       `json:"created"`
    key             []byte                      `json:"k"`
    publicKey       plan.IdentityPublicKey     // Only set iff .keyType == AsymmetricKey
}


func (entry *KeyEntry) EqualTo(other KeyEntry) bool {
    return entry.keyType != other.keyType ||
        entry.creationTime != other.creationTime ||
        bytes.Equal(entry.key, other.key) == false ||
        bytes.Equal(entry.publicKey,other.publicKey) == false ||
        entry.keyID != other.keyID

}






type Keyring struct {
    sync.RWMutex
    
    name            string
    keysByID        map[plan.KeyID]KeyEntry
}

func NewKeyring() *Keyring {

    return &Keyring{
        keysByID: map[plan.KeyID]KeyEntry{},
    }
}




// NewIdentity generates encryption and signing keys, adds them to the
// Keyring, and returns the public keys associated with those private
// keys.
func (kr *Keyring) NewIdentity() (outSigningKey plan.IdentityPublicKey, outEncKey plan.IdentityPublicKey) {

    encrKey := GenerateKeyEntry(EncryptionKey)
    signKey := GenerateKeyEntry(SigningKey)

	// store it in the Keyring and return the public keys
	kr.Lock()
	kr.keysByID[encrKey.keyID] = encrKey
    kr.keysByID[signKey.keyID] = signKey
    kr.Unlock()

	return encrKey.publicKey, signKey.publicKey
}


// InstallKey adds a key to the keychain (ignoring collitions if the jey entry is identical)
func (kr *Keyring) AddKeys(
    inEntries []KeyEntry,
    ) *plan.Perror {

    var collisions []plan.KeyID

    kr.Lock()
    for _, entry := range inEntries {
        existing, ok := kr.keysByID[entry.keyID]
        if ok && ! existing.EqualTo(entry) {
            collisions = append(collisions, entry.keyID)
        } else {
            kr.keysByID[entry.keyID] = entry
        }
    }
    kr.Unlock()

    var err *plan.Perror

    if len(collisions) > 0 {
        err = plan.Errorf(nil, plan.KeyIDCollision, "key ID collision while adding keys {keyID:%v}", collisions)
    }
    return err

}



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




// GetSigningKey fetches the d's private signing key from the keychain for a
// specific public key, or an error if the key doesn't exist.
func (kr *Keyring) GetSigningKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {

	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()
    
	if !ok || entry.keyType != SigningKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "signing key not found {keyID:%v}", inKeyID)
	}
	return entry.key, nil
}



// GetEncryptKey fetches the d's private encrypt key from the keychain,
// or an error if the key doesn't exist.
func (kr *Keyring) GetEncryptKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {
    
	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()

	if !ok || entry.keyType != EncryptionKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "encrypt key not found {keyID:%v}", inKeyID)
	}
	return entry.key, nil
}



// GetSymmetricKey fetches the community key from the keychain for a
// based on its ID, or an error if the key doesn't exist.
func (kr *Keyring) GetSymmetricKey(inKeyID plan.KeyID) (
	[]byte, *plan.Perror) {
    
	kr.RLock()
    entry, ok := kr.keysByID[inKeyID]
    kr.RUnlock()

	if !ok || entry.keyType != SymmetricKey {
		return nil, plan.Errorf(nil, plan.KeyIDNotFound, "community key not found {keyID:%v}", inKeyID)
	}
	return entry.key, nil
}





func GenerateKeyEntry(inKeyType KeyType) KeyEntry {
    entry := KeyEntry{
        keyType: inKeyType,
        creationTime: plan.Now().UnixSecs,
    }

    switch inKeyType {

        case SymmetricKey:{
            entry.key = make([]byte, 32)
            _, err := crypto_rand.Read(entry.key)
            if err != nil {
                panic(err)
            }

            _, err = crypto_rand.Read(entry.keyID[:])
            if err != nil {
                panic(err)
            }
        }

        case EncryptionKey:{
            publicKey, privateKey, err := box.GenerateKey(crypto_rand.Reader)
            if err != nil {
                panic(err)
            }
            entry.key = privateKey[:]
            entry.publicKey = publicKey[:]
            entry.keyID.AssignFrom(entry.publicKey)
        }

        case SigningKey:{
            publicKey, privateKey, err := sign.GenerateKey(crypto_rand.Reader)
            if err != nil {
                panic(err)
            }
            entry.key = privateKey[:]
            entry.publicKey = publicKey[:]
            entry.keyID.AssignFrom(entry.publicKey)
        }
    }

    return entry

}




// ---------------------------------------------------------
// community key functions
//

// NewSymmetricKey generates a new community key, adds it to the Keyring,
// and returns the CommunityKeyID associated with that key.
func (kr *Keyring) NewSymmetricKey() plan.KeyID {
	
    symKey := GenerateKeyEntry(SymmetricKey)

    kr.Lock()
    kr.keysByID[symKey.keyID] = symKey
    kr.Unlock()

	return symKey.keyID
}

