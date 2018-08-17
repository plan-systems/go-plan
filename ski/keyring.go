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
	communityKeys map[plan.KeyID][]byte
	signingKeys   map[plan.IdentityPublicKey]*[64]byte
	encryptKeys   map[plan.IdentityPublicKey]*[32]byte
	mux           sync.RWMutex // synchronized changes to the keyring
}

func newKeyring() *keyring {
	return &keyring{
		communityKeys: map[plan.KeyID][]byte{},
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
	kr.signingKeys[signingPubKey] = signingPrivateKey
    kr.encryptKeys[encryptPubKey] = encryptPrivateKey
    kr.mux.Unlock()

	return encryptPubKey, signingPubKey
}

// GetSigningKey fetches the d's private signing key from the keychain for a
// specific public key, or an error if the key doesn't exist.
func (kr *keyring) GetSigningKey(pubKey plan.IdentityPublicKey) (
	*[64]byte, *plan.Perror) {
    var key *[64]byte
    
	kr.mux.RLock()
    key, ok := kr.signingKeys[pubKey]
    kr.mux.RUnlock()
    
	if !ok {
		return nil, plan.Errorf(nil, plan.SigningKeyNotFound, "signing key not found {pubKey:%v}", pubKey)
	}
	return key, nil
}

// GetEncryptKey fetches the d's private encrypt key from the keychain,
// or an error if the key doesn't exist.
func (kr *keyring) GetEncryptKey(pubKey plan.IdentityPublicKey) (
	*[32]byte, *plan.Perror) {
    var key *[32]byte
    
	kr.mux.RLock()
    key, ok := kr.encryptKeys[pubKey]
    kr.mux.RUnlock()

	if !ok {
		return nil, plan.Errorf(nil, plan.EncryptKeyNotFound, "encrypt key not found {pubKey:%v}", pubKey)
	}
	return key, nil
}

// Removes any instance of a key associated with the public key provided
// from the keyring
func (kr *keyring) InvalidateIdentity(key plan.IdentityPublicKey) {
    
	kr.mux.Lock()
	delete(kr.signingKeys, key)
    delete(kr.encryptKeys, key)
    kr.mux.Unlock()
}

func generateEncryptionKey() (plan.IdentityPublicKey, *[32]byte) {
	publicKey, privateKey, err := box.GenerateKey(crypto_rand.Reader)
	if err != nil {
		panic(err)
	}
	return plan.NewIdentityPublicKey(publicKey), privateKey
}

func generateSigningKey() (plan.IdentityPublicKey, *[64]byte) {
	publicKey, privateKey, err := sign.GenerateKey(crypto_rand.Reader)
	if err != nil {
		panic(err)
	}
	return plan.NewIdentityPublicKey(publicKey), privateKey
}

// ---------------------------------------------------------
// community key functions
//

// NewCommunityKey generates a new community key, adds it to the keyring,
// and returns the CommunityKeyID associated with that key.
func (kr *keyring) NewCommunityKey() plan.KeyID {
	
    key, keyID := generateSymmetricKey()
    
    kr.mux.Lock()
    kr.communityKeys[keyID] = key[:]
    kr.mux.Unlock()

	return keyID
}

// InstallCommunityKey adds a new community key to the keychain
func (kr *keyring) InstallCommunityKey(
	keyID plan.KeyID, key []byte) {

	kr.mux.Lock()
    kr.communityKeys[keyID] = key
    kr.mux.Unlock()
}

// GetCommunityKeyByID fetches the community key from the keychain for a
// based on its ID, or an error if the key doesn't exist.
func (kr *keyring) GetCommunityKeyByID(keyID plan.KeyID) (
	[]byte, *plan.Perror) {
    
	kr.mux.RLock()
    key, ok := kr.communityKeys[keyID]
    kr.mux.RUnlock()

	if !ok {
		return nil, plan.Errorf(nil, plan.CommunityKeyNotFound, "community key not found {keyID:%v}", keyID)
	}
	return key, nil
}

func generateSymmetricKey() ([32]byte, plan.KeyID) {
	secret := make([]byte, 32)
	_, err := crypto_rand.Read(secret)
	if err != nil {
		panic(err) // TODO: unclear when we'd ever hit this?
	}
	var key [32]byte
	copy(key[:32], secret[:])

	keyID := make([]byte, 16) // TODO: is this enough for uniqueness?
	_, err = crypto_rand.Read(keyID)
	if err != nil {
		panic(err) // TODO: unclear when we'd ever hit this?
	}
	var communityID plan.KeyID
	copy(communityID[:16], keyID[:])

	return key, communityID
}
