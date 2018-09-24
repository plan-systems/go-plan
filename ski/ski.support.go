
package ski


import (
    "bytes"

	"github.com/plan-tools/go-plan/plan"
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





