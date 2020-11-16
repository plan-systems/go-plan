// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

/*****************************************************
** ski.Session
**/

// Session provides crypto services from an opaque service provider.
type Session interface {

	// Merges all keys in the given KeyTome with this host KeyTome.
	// See docs for KeyTome.MergeTome() on how error conditions are addressed.
	// Note: incoming duplicate key entries are ignored/dropped.
	//ImportKeys(srcTome *KeyTome) error

	// Generates a new KeyEntry for each entry in srcTome (based on the entry's KeyType and CryptoKitID, ignoring the rest) and merges it
	// with the host KeyTome. A copy of each newly generated entry (except for PrivKey) is placed into result KeyTome.
	// See "KeyGen mode" notes where KeyEntry is declared.
	GenerateKeys(srcTome *KeyTome) (*KeyTome, error)

	// Returns a info about a key for the referenced key.
	// If len(inKeyRef.PubKey) == 0, then the newest KeyEntry in the implied Keyring is returned.
	FetchKeyInfo(inKeyRef *KeyRef) (*KeyInfo, error)

	// Performs signing, encryption, and decryption.
	DoCryptOp(inArgs *CryptOpArgs) (*CryptOpOut, error)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(reason string)
}

/*****************************************************
** ski.Provider AccessScopes
**
** FUTURE
**/

// AccessScopes specifies a set of SKI ops allowed to occur in a SKI session.  A clients requests this set when starting a new session,
//     allowing the SKI provider to provision for the requested access (e.g. getting user permission of not already granted).
type AccessScopes []string

const (

	// ContentDecryptAccess only allows the client to decrypt data
	ContentDecryptAccess = "ContentDecryptAccess"

	// ContentAuthoringAccess allows the client to encrypt and sign data
	ContentAuthoringAccess = "ContentDecryptAccess"

	// AddKeysAccess allows keys to be imported and generated
	AddKeysAccess = "AddKeysAccess"
)
