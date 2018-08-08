package plan

import (
	"fmt"
	//"encoding/base64"

	"github.com/ethereum/go-ethereum/crypto/sha3"
)

// PError is PLAN's commonly used error type
// https://github.com/golang/go/wiki/Errors
type PError struct {
	code int32
	msg  string
}

// Error create a new Perror
func Error(inCode int32, inMsg string) error {
	return &PError{
		inCode,
		inMsg,
	}
}

// Errorf is a convenience function of Error() that uses a string formatter.
func Errorf(inCode int32, inFormat string, inArgs ...interface{}) error {
	return &PError{
		inCode,
		fmt.Sprintf(inFormat, inArgs),
	}
}

func (e *PError) Error() string {
	return fmt.Sprintf("%s {code=%d}", e.msg, e.code)
}

const (

	// IdentityAddrSz is the numbewr of bytes in most PLAN addresses and public IDs.  It's the right-most bytes of a longer public key.
	// Background on probability of address collision for 20 bytes (160 bits) http://preshing.com/20110504/hash-collision-probabilities/
	IdentityAddrSz = 20
)

// CommunityID identifies a PLAN community
type CommunityID [IdentityAddrSz]byte

// IdentityPublicKey is the public key of a person, group, or sub community inside a PLAN commiunity.
type IdentityPublicKey [32]byte

// IdentityAddr the rightmost bytes of IdentityPublicKey
type IdentityAddr [IdentityAddrSz]byte

// PDIEntrySig holds a final signature of a PDI entry, veryifying the author's private key was used to sign the entry
type PDIEntrySig [20]byte

// PDIEntryHash is a hash digest of a PDI entry.
type PDIEntryHash [32]byte

// ChannelID identifies a speicifc PLAN channel where PDI entries are posted to.
type ChannelID [20]byte

// CommunityKey is an symmetric key that allows community data to be encrypted/decrypted.
type CommunityKey [32]byte

// EncryptedChannelKey is an encrypted symmetric key used by PLAN clients to encode/decode PDI entries posted to private (encrypted) channels.
type EncryptedChannelKey []byte

// Nonce is a number used once globally as part of both symmetric and
// public key encryption
type Nonce [24]byte

var (

	// RootAccessChannel is the community's root access-level channel, meaning this channel effectively
	//    specifies which community members are "community admins".  All other channels and access channels
	//    are ultimately controlled by the community members listed in this root channel.  This means
	//    the hierarchy of access channels is rooted in this channel.
	// Note that the parent access channel is set to itself by default.
	RootAccessChannel = ChannelID{
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 1,
	}

	// MemberRegistryChannel is the community's master (community-public) member registry.  Each entry specifies a
	//    each community member or group's community ID, latest public key, and resource quotas.  This allows each of the
	//    community's pnodes to verify member signatures and enable the passing of secrets to other members or groups
	//    via asymmetric encryption. Naturally, this channel is controlled by an access channel that is controlled only
	//    by community admins and is set to RootAccessChannel by default.  Since each entry in this channel represents
	//    an official community record (that only a community admin can edit), entries can also contain additional
	//    information desired that community admins wish (or require) to be publicly available (and unforgeable).
	// Note how a member's ID can always be remapped to any number of deterministically generated channel IDs.  For
	//    example, by convention, a member's /plan/member "home channel" is implicitly specified by virtue of knowing
	//    a member's community member ID (since a community member ID never changes)
	MemberRegistryChannel = ChannelID{
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 2,
	}

	// ChannelCatalogChannel is where the existence of community-public channels is communicated to other community members.
	// By default, only community admins can post to this channel, ensuring community admins decide what channels are readily
	//     visible to other community though access can be granted to other select users.  This channel allows users to find
	//     any community-public channel, regardless if the channel has been linked in a public workspace.
	ChannelCatalogChannel = ChannelID{
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 3,
	}
)

/*
A CommunityKeyID identifies a specific shared "community-global" symmetric key.
When a PLAN client starts a session with a pnode, the client sends the pnode her community-public keys.
PDIEntryCrypt.CommunityKeyID specifies which community key was used to encrypt PDIEntryCrypt.Header.
If/when an admin of a community issues a new community key,  each member is securely sent this new key via
the community key channel (where is key is asymmetrically sent to each member still "in" the community.  */
type CommunityKeyID [16]byte

/*
AccessChannelID references an access log of community identities that have been
granted read, write, or own access to channels bearing that given AccessChannelID. */
type AccessChannelID ChannelID

// PDIEntryVerb is the top-level PDI signal that specifies one of the low-level, built-in purposes of a PDI entry.
type PDIEntryVerb int32

const (

	// PDIEntryVerbChannelAdmin creates a new channel (not common)
	PDIEntryVerbChannelAdmin = 1

	// PDIEntryVerbPostEntry posts this entry body to the specified channel
	PDIEntryVerbPostEntry = 2

	// PDIEntryVerbReplaceEntry replaces the cited entry with this one in its place
	PDIEntryVerbReplaceEntry = 3
)

// PDIEntryInfo contains flags and info about this entry, in effect specifying which hash functions and crypto to apply.
type PDIEntryInfo [4]byte

// PDIEntryVers dictates the serialization format of this entry post
type PDIEntryVers byte

const (

	// PDIEntryVers1 default schemea
	PDIEntryVers1 byte = 1 + iota
)

// Time is a unix time (UTC in seconds elapsed since Jan 1, 1970)
type Time int64

const (

	// DistantFuture is a const used to express the distant future
	DistantFuture Time = (1 << 62)

	// DistantPast is a const used to express the distant past
	DistantPast Time = -(1 << 62)
)

/*
 type PDIEntrySchema struct {
    Hasher                  hash.Hash
    Signer                  interface{}
 }



 var PDIEntrySchemas = [2]PDIEntrySchema {
    PDIEntrySchema{
    },
    PDIEntrySchema {
        Hasher:     hash.SHA256,
        Signer:     nil,
    },
}


*/

/*
PDIEntryCrypt is the public "wire" format for PDI entrues.  It is what it sent to/from communuity data store implementations,
such as Ethereum and NEM.

A PDI entry has two encrypted segements, its header and body segment.  The PDIEntryCrypt.Header is encrypted using a community-public key,
specified by PDIEntryCrypt.CommunityKeyID.  This ensures non-community members (i.e. public snoops) can't see any entry params,
data, or even to what community channel the entry is being post to.  PDIEntryCrypt.Body is encrypted by the same community key as the header *or*
by the key specified via PDIEntryHeader.Author and PDIEntry.AccessChannelID (all via the client's Secure Key Interface).  In sum,
PDIEntry.AccessChannelID specifies a decryption flow through the user's private, compartmentalized keychain.

When a PLAN client starts a new session with a pnode, the client sends the community keys for the session.  This allows
pnode to process and decrypt incoming PDIEntryCrypt entries from the storage medium (e.g. Ethereum).  Otherwise, pnode
has no ability to decrypt PDIEntryCrypt.Header.  A pnode could be configured to keep the communty keychain even when there
are no open client PLAN sessions so that incomning entries can be processed.  Otherwise, incoming entries won't be processed
from the lowest level PDI storage layer.  Both configurations are reasonable depending on security preferences.

When a community admin initiates a community-key "rekey event", the newly generated community key is securely and individually
"sent" to each community member via the community's public key transfer channel. The new community key is encrypted using each member's public key.
When a pnode is processing an entry that it does not have a community key for, it will check the community's public key channel
for an entry for the current client's public key, it will send the client the encrypted community key.  The client uses its SKI
to decrypt the payload into the new community key.  This key is added to the user's SKI keychain and is sent back to pnode.

When pnode sends a PLAN client is PDI entries, it sends PDIEntry.Header (via json) and PDIEntryCrypt.Body.  If the

Recall that the pnode client has no abilty to decrypt PDIEntryCrypt.Body if PDIEntryHeader.AccessChannelID isn't set for
community-public permissions.  This is fine since only PLAN clients with a

And awaaaaay we go!
*/
type PDIEntryCrypt struct {
	Sig  PDIEntrySig   // Signature of PDIEntry.Hash (signed by PDIEntry.Header.Author)
	Hash *PDIEntryHash // IF set, set to self.ComputeHash()

	Info           PDIEntryInfo // Entry type info, Allows PDIEntry accessors to apply the correct hash and crypto functions
	CommunityKeyID CommunityKeyID

	HeaderCrypt []byte // Encrypted using a community key, referenced via .CommunityKeyID
	BodyCrypt   []byte // Encrypted using a community key or a user private key (based on PDIEntry.Header.AccessChannelID)

}

// PDIEntry is wha
type PDIEntry struct {
	PDIEntryCrypt *PDIEntryCrypt // Originating (encrypted) entry data used to instantiate this data

	HeaderBuf []byte // Serialized representation of PDIEntry.Header  (decrypted form PDIEntryCrypt.Header)
	Header    *PDIEntryHeader

	BodyBuf []byte // Serialized representation of PDIEntry.Body (decrypted from PDIEntryCrypt.Body)
	Body    *PDIEntryBody
}

// PDIEntryHeader is a container for community-public info about this channel entry.PDIEntryHeader
type PDIEntryHeader struct {
	Nonce            Nonce // Prevents replay attacks -- should be unique for each entry sealed
	Time             Time
	Verb             PDIEntryVerb      // PDI command/verb
	ChannelID        ChannelID         // The channel id this entry is posted to.
	Author           IdentityAddr      // Creator of this entry (and signer of .Sig)
	AccessChannelID  AccessChannelID   // Specifies the permissions channel (an access control implemenation) this entry was encrypted with
	AccessChannelRev uint32            // Specifies what identifying major rev of the access channel was in effect when this entry was authored
	Params           map[string]string // Additional param list (PDI-level key-value params) -- PDI_param_*: <value>
}

// PDIEntryBody is the decrypted and deserialized form of PDIEntryCrypt.Body and an abstract data container.
type PDIEntryBody struct {
	Nonce     Nonce         // Prevents replay attacks -- should be unique for each entry sealed
	BodyParts []PDIBodyPart // Zero or more data sections -- e.g. .parts[0] is a serialized json param list and .parts[1] is a blob of rich text (rtf).
}

// PDIBodyPart is one of one or more sequential parts of a PDIEntryBody.PDIBodyPart.
// Each part has its own multistream-codec style description header ("/protocol/sub-protocol")
type PDIBodyPart struct {
	Header string // <multistream-codec-header> -- Human-readable, UTF8 string that describes .body (e.g. "/json/rpc")
	Body   []byte // Arbitrary client binary data conforming to .header
}

// When a new PDIEntry arrives off the wire, .Sig is set, .Hash == nil, .dataCrypt is set, .Body == nil
//    1) .dataCrypt is hashed via  PDIEntrySchema.BodyHasher
//    2) .Sig is verfified based on the hash calculated from (1)
//    3) Use .AccessChannelID to specifiy a decryption flow through PLAN's Secure Key Interface (SKI).
//         - If the local user pubkey does not have "read" access, then that means there's a db corresponding to that access list that contains a list of all entries
//           that have been encountered (and can't be unlocked).  In this case, referencing info for this entry is appended to this db and entry is not
//           processed further.  If/when the CommunityRepo sees an access control list get unlocked for the current logged in user, it will
//         -
// , then the entry data can only be stored until the user is granted access.
//    2) Compute Hash of
//    2) Decrypt .dataCrypt into .dataRLP (a generic blob)
//    3) Deserialize this output blob (instantiate .Body).

// Assert is PLAN's easy assert
func Assert(inCond bool, inFormat string, inArgs ...interface{}) {

	if !inCond {
		panic(fmt.Sprintf(inFormat, inArgs))
	}
}

// ComputeHash hashes all fields of PDIEntryCrypt (except .Sig)
func (inEntry *PDIEntryCrypt) ComputeHash(outHash *PDIEntryHash) {

	hw := sha3.NewKeccak256()
	hw.Write(inEntry.Info[:])
	hw.Write(inEntry.CommunityKeyID[:])
	hw.Write(inEntry.HeaderCrypt)
	hw.Write(inEntry.BodyCrypt)

	out := hw.Sum(outHash[:0])
	copy(outHash[:32], out[:])
}

// ChannelProperties specifies req'd params for all channels and are community-public.
// Note that some params are immutable once they are set in the channel genesis block (e.g. IsAccessChannel and EntriesAreFinal)
type ChannelProperties struct {

	// Who issued this set of channel properties
	Author IdentityAddr `json:"author"`

	// Does this channel conform to the requirements needed so that this channel can be used to control access for another channel.
	// An access channel's purpose is to:
	//     (a) distribute private channel keys to members (by encrypting a private channel key using a member's public key)
	//     (b) publish write and own privs to others (only permitted by owners)
	IsAccessChannel bool `json:"isAccessChannel"`

	// Are entries in this channel allowed be revoked or superceeded?
	EntriesAreFinal bool `json:"entriesFinal"`

	// This channel's ID
	ChannelID ChannelID `json:"chID"`

	// Specifies an owning access channel that asserts domain over this channel
	OwningAccessChannelID AccessChannelID `json:"acID"`

	// Specifies which major revision number of the owning access channel was in effect when this entry was authored.
	// In general, a pnode won't insert/approve of a PDI entry until/unless the access channel revisions match.
	OwningAccessChannelRev uint32 `json:"acRev"`

	// Complete set of channel params
	Params map[string]interface{} `json:"params"`
}

/*
type ChannelInfo struct {

    // Specifies when this channel was created
    TimeCreated             Time                            `json:"inEffect"`

    // <<multistream>-inspired description of this channel's contents.  This allows the PLAN client to accurately process, interpret, and handle entry
    //    entry data blobs (PDIEntry.Body -- i.e. PDIEntryBody).  example:  "/plan/talk/v2"
    Protocol                string                          `json:"protocol"`

    // What is the title of this channel presented to pariticipants?
    ChannelTitle            string                          `json:"chTitle"`

    // What is the description of this channel preseted to pariticipants?
    ChannelDesc             string                          `json:"chDesc"`

}
*/

/*
Opening a private message channel flow where Alice wants to talk to Bob:
1) Alice uses UI to open a "private message channel" to Bob
2) PLAN makes a new channel named "/talk/<Bob>/<title>" and it is encrypted based on the new ACL and published)
4a)  Some time later, Bob's client gets the new entries from the network
4bc) Bob's pnode sees a new channel appear having an ACL that he has access to
4bc) Bob's pnode sees a an ACL appear that his pubkey has access to
5) Bob's pnode is able to process and decrypt new posts made to this new channel
6) Bob's PLAN client gets the new posts to master-community ACL and master channel registry
7) Bob's client notifies Bob that he has been given access to a new channel.
*/
