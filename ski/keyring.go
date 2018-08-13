package ski // import "github.com/plan-tools/go-plan/ski"

import (
	crypto_rand "crypto/rand"
	"sync"

	plan "github.com/plan-tools/go-plan/plan"
	box "golang.org/x/crypto/nacl/box"
	sign "golang.org/x/crypto/nacl/sign"
)

// keyring represents the storage of keys
type keyring struct {
	communityKeys map[plan.CommunityKeyID]plan.CommunityKey
	signingKeys   map[plan.IdentityPublicKey]*[64]byte
	encryptKeys   map[plan.IdentityPublicKey]*[32]byte
	mux           sync.RWMutex // synchronized changes to the keyring
}

func newKeyring() *keyring {
	return &keyring{
		communityKeys: map[plan.CommunityKeyID]plan.CommunityKey{},
		signingKeys:   map[plan.IdentityPublicKey]*[64]byte{},
		encryptKeys:   map[plan.IdentityPublicKey]*[32]byte{},
	}
}

// ---------------------------------------------------------
// self identity functions
//

// NewIdentity generates encryption and signing keys, adds them to the
// keyring, and returns the public keys associated with those private
// keys.
func (kr *keyring) NewIdentity() (plan.IdentityPublicKey, plan.IdentityPublicKey) {

	// generate new key material
	encryptPubKey, encryptPrivateKey := generateEncryptionKey()
	signingPubKey, signingPrivateKey := generateSigningKey()

	// store it in the keyring and return the public keys
	kr.mux.Lock()
	defer kr.mux.Unlock()
	kr.signingKeys[signingPubKey] = signingPrivateKey
	kr.encryptKeys[encryptPubKey] = encryptPrivateKey
	return encryptPubKey, signingPubKey
}

// GetSigningKey fetches the user's private signing key from the keychain for a
// specific public key, or an error if the key doesn't exist.
func (kr *keyring) GetSigningKey(pubKey plan.IdentityPublicKey) (
	*[64]byte, error) {
	var key *[64]byte
	kr.mux.RLock()
	defer kr.mux.RUnlock()
	key, ok := kr.signingKeys[pubKey]
	if !ok {
		return key, plan.Errorf(-1,
			"GetSigningKey: signing key %v does not exist", pubKey)
	}
	return key, nil
}

// GetEncryptKey fetches the user's private encrypt key from the keychain,
// or an error if the key doesn't exist.
func (kr *keyring) GetEncryptKey(pubKey plan.IdentityPublicKey) (
	*[32]byte, error) {
	var key *[32]byte
	kr.mux.RLock()
	defer kr.mux.RUnlock()
	key, ok := kr.encryptKeys[pubKey]
	if !ok {
		return key, plan.Errorf(-1,
			"GetEncryptKey: encrypt key %v does not exist", pubKey)
	}
	return key, nil
}

// Removes any instance of a key associated with the public key provided
// from the keyring
func (kr *keyring) InvalidateIdentity(key plan.IdentityPublicKey) {
	kr.mux.Lock()
	defer kr.mux.Unlock()
	delete(kr.signingKeys, key)
	delete(kr.encryptKeys, key)
}

func generateEncryptionKey() (plan.IdentityPublicKey, *[32]byte) {
	publicKey, privateKey, err := box.GenerateKey(crypto_rand.Reader)
	if err != nil {
		panic(err)
	}
	return newPubKey(publicKey), privateKey
}

func generateSigningKey() (plan.IdentityPublicKey, *[64]byte) {
	publicKey, privateKey, err := sign.GenerateKey(crypto_rand.Reader)
	if err != nil {
		panic(err)
	}
	return newPubKey(publicKey), privateKey
}

// ---------------------------------------------------------
// community key functions
//

// NewCommunityKey generates a new community key, adds it to the keyring,
// and returns the CommunityKeyID associated with that key.
func (kr *keyring) NewCommunityKey() plan.CommunityKeyID {
	kr.mux.Lock()
	defer kr.mux.Unlock()
	key, keyId := generateSymmetricKey()
	kr.communityKeys[keyId] = key
	return keyId
}

// InstallCommunityKey adds a new community key to the keychain
func (kr *keyring) InstallCommunityKey(
	keyId plan.CommunityKeyID, key plan.CommunityKey) {
	kr.mux.Lock()
	defer kr.mux.Unlock()
	kr.communityKeys[keyId] = key
}

// GetCommunityKeyByID fetches the community key from the keychain for a
// based on its ID, or an error if the key doesn't exist.
func (kr *keyring) GetCommunityKeyByID(keyId plan.CommunityKeyID) (
	plan.CommunityKey, error) {
	var key plan.CommunityKey
	kr.mux.RLock()
	defer kr.mux.RUnlock()
	key, ok := kr.communityKeys[keyId]
	if !ok {
		return key, plan.Errorf(-1,
			"GetCommunityKeyByID: community key %v does not exist", keyId)
	}
	return key, nil
}

func generateSymmetricKey() ([32]byte, plan.CommunityKeyID) {
	secret := make([]byte, 32)
	_, err := crypto_rand.Read(secret)
	if err != nil {
		panic(err) // TODO: unclear when we'd ever hit this?
	}
	var key [32]byte
	copy(key[:32], secret[:])

	keyId := make([]byte, 16) // TODO: is this enough for uniqueness?
	_, err = crypto_rand.Read(keyId)
	if err != nil {
		panic(err) // TODO: unclear when we'd ever hit this?
	}
	var communityId plan.CommunityKeyID
	copy(communityId[:16], keyId[:])

	return key, communityId
}
