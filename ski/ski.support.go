package ski

import (
	"bytes"

	"github.com/plan-tools/go-plan/plan"
)

const (
	// KeyTypeShift specifies how many bits are reserved to store a key's KeyType
	KeyTypeShift = 3

	// KeyTypeMask is a mask based on KeyTypeShift
	KeyTypeMask = (1 << KeyTypeShift) - 1
)

// EqualTo compares if two key entries are identical/interchangable
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
	return entry.KeyInfo != other.KeyInfo ||
		entry.TimeCreated != other.TimeCreated ||
		bytes.Equal(entry.PrivKey, other.PrivKey) == false ||
		bytes.Equal(entry.PubKey, other.PubKey) == false

}

// GetKeyID is a convenience function that returns this key's fixed ID (determined by the key's public key)
func (entry *KeyEntry) GetKeyID() plan.KeyID {
	return plan.GetKeyID(entry.PubKey)
}

// KeyType returns the key's KeyType
func (entry *KeyEntry) KeyType() KeyType {
	return KeyType(entry.KeyInfo & KeyTypeMask)
}

// CryptoPkgID returns the crypto package ID this key is used with
func (entry *KeyEntry) CryptoPkgID() CryptoPkgID {
	return CryptoPkgID(entry.KeyInfo >> KeyTypeShift)
}
