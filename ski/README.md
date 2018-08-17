# ski
--
    import "github.com/plan-tools/go-plan/ski"

Package ski is a reference implementation of the SKI plugin

## Usage

#### type SKI

```go
type SKI struct {
}
```

SKI represents the external SKI process and holds the keyring.

#### func  NewSKI

```go
func NewSKI() *SKI
```
NewSKI initializes the SKI's keying.

#### func (*SKI) AcceptVouch

```go
func (ski *SKI) AcceptVouch(
	recvPubKey plan.IdentityPublicKey,
	bodyCrypt []byte,
	senderPubKey plan.IdentityPublicKey,
) error
```
AcceptVouch decrypts the encrypted buffer written by Vouch and decrypts it for
the recipient.

#### func (*SKI) Decrypt

```go
func (ski *SKI) Decrypt(
	keyID plan.KeyID,
	encrypted []byte,
) ([]byte, error)
```
Decrypt takes an encrypted buffer and decrypts it using the community key and
returns the cleartext buffer (or an error).

#### func (*SKI) DecryptFrom

```go
func (ski *SKI) DecryptFrom(
	recvPubKey plan.IdentityPublicKey,
	encrypted []byte,
	senderPubKey plan.IdentityPublicKey,
) ([]byte, error)
```
DecryptFrom takes an encrypted buffer and a public key, and decrypts the message
using the recipients private key. It returns the decrypted buffer (or an error).

#### func (*SKI) Encrypt

```go
func (ski *SKI) Encrypt(keyId plan.KeyID, msg []byte,
) ([]byte, error)
```
Encrypt accepts a buffer and encrypts it with the community key and returns the
encrypted buffer (or an error). Typically the msg buffer will be a serialized
PDIEntryBody or PDIEntryHeader. This is authenticated encryption but the caller
will follow this call with a call to Verify the PDIEntryHash for validation.

#### func (*SKI) EncryptFor

```go
func (ski *SKI) EncryptFor(
	senderPubKey plan.IdentityPublicKey,
	msg []byte,
	recvPubKey plan.IdentityPublicKey,
) ([]byte, error)
```
EncryptFor accepts a buffer and encrypts it for the public key of the intended
recipient and returns the encrypted buffer. Typically the msg buffer will be a
serialized PDIEntryBody or PDIEntryHeader. Note: this is how the Vouch operation
works under the hood except that the Vouch caller doesn't know what goes in the
message body. Outside of Vouch operations, this is the basis of private messages
between users. The caller will follow this call with a call to Sign the
PDIEntryHash

#### func (*SKI) NewCommunityKey

```go
func (ski *SKI) NewCommunityKey() plan.KeyID
```
NewCommunityKey generates a new community key, adds it to the keyring, and
returns the CommunityKeyID associated with that key.

#### func (*SKI) NewIdentity

```go
func (ski *SKI) NewIdentity() (
	plan.IdentityPublicKey, plan.IdentityPublicKey)
```
NewIdentity generates encryption and signing keys, adds them to the keyring, and
returns the public keys associated with those private keys as (encryption,
signing).

#### func (*SKI) Sign

```go
func (ski *SKI) Sign(signer plan.IdentityPublicKey, hash plan.PDIEntryHash,
) (plan.PDIEntrySig, error)
```
Sign accepts a message hash and returns a signature.

#### func (*SKI) Verify

```go
func (ski *SKI) Verify(
	pubKey plan.IdentityPublicKey,
	hash plan.PDIEntryHash,
	sig plan.PDIEntrySig,
) ([]byte, bool)
```
Verify accepts a signature and verfies it against the public key of the sender.
Returns the verified buffer (so it can be compared by the caller) and a bool
indicating success.

#### func (*SKI) Vouch

```go
func (ski *SKI) Vouch(
	communityKeyID plan.KeyID,
	senderPubKey plan.IdentityPublicKey,
	recvPubKey plan.IdentityPublicKey,
) ([]byte, error)
```
Vouch encrypts a CommunityKey for the recipients public encryption key, and
returns the encrypted buffer (or error)
