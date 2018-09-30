package ski

import (
	"bytes"
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
