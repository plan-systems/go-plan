// Package ski (Secure Key Interface) contains PLAN's crypto abstractions and pluggable interfaces
package ski

import (

)

/*****************************************************
** ski.Session
**/

// Session provides crypto services from an opaque service provider.
//
// FUTURE: this will become a gRPC service.
type Session interface {

    // Generates a new KeyEntry for each entry in srcTome (based on the entry's KeyType and CryptoKitID, ignoring the rest) and merges it
    //    with the host KeyTome. A copy of each newly generated entry (except for PrivKey) is placed into result KeyTome.
    // See "KeyGen mode" notes where KeyEntry is declared. 
    GenerateKeys(srcTome *KeyTome) (*KeyTome, error)
    
    // Returns a info about a key for the referenced key
    // If len(inKeyRef.PubKey) == 0, then the newest KeyEntry in the implied Keyring is returned.
    FetchKeyInfo(inKeyRef *KeyRef) (*KeyInfo, error)

    // Performs signing, encryption, and decryption.
    DoCryptOp(inArgs *CryptOpArgs) (*CryptOpOut, error)

	// EndSession ends this SKI session, resulting in the host Provider to call its inOnSessionEnded() callback followed by inOnCompletion.
	// Following a call to EndSession(), no more references to this session should be made.
	EndSession(inReason string)
}

/*****************************************************
** ski.Provider AccessScopes
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
