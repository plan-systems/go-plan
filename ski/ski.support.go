package ski

import (
    "io"
	"bytes"

	"github.com/plan-tools/go-plan/plan"
)


// EqualTo compares if two key entries are identical/interchangable
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
	return entry.KeyType != other.KeyType ||
		entry.KeyDomain != other.KeyDomain ||
		entry.CryptoKitId != other.CryptoKitId ||
		entry.TimeCreated != other.TimeCreated ||
		bytes.Equal(entry.PrivKey, other.PrivKey) == false ||
		bytes.Equal(entry.PubKey, other.PubKey) == false

}

// GetKeyID returns the KeyID for this KeyEntry
func (entry *KeyEntry) GetKeyID() plan.KeyID {
	return plan.GetKeyID(entry.PubKey)
}


// GenerateNewKeys is a convenience bulk function for CryptoKit.GenerateNewKey()
func GenerateNewKeys(
    inRand io.Reader,
    inRequestedKeyLen int,
    inKeyReqs []*KeyEntry,
) ([]*KeyEntry, *plan.Perror) {

    N :=  len(inKeyReqs)

    newKeys := make([]*KeyEntry, N)

    var kit *CryptoKit 
    var err *plan.Perror

    timeCreated := plan.Now().UnixSecs

    for i, keyReq := range inKeyReqs {

        if kit == nil || kit.CryptoKitID != keyReq.CryptoKitId {
            kit, err = GetCryptoKit(keyReq.CryptoKitId)
            if err != nil {
                return nil, err
            }
        }

        newKey := &KeyEntry{
            KeyType: keyReq.KeyType,
            KeyDomain: keyReq.KeyDomain,
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