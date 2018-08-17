package plan

import (
    "fmt"
    "time"
    "net/http"

	"github.com/ethereum/go-ethereum/crypto/sha3"
)


// DataHandler is a deferred data handler function
type DataHandler            func(inErr *Perror, inParam []byte)

// Action is a deferred generic handler. 
type Action                 func(inErr *Perror, inParam interface{})






const (

	// IdentityAddrSz is the number of bytes in most PLAN addresses and public IDs.  It's the right-most bytes of a longer public key.
	// Background on probability of address collision for 20 bytes (160 bits) http://preshing.com/20110504/hash-collision-probabilities/
    IdentityAddrSz = 20
    
    // KeyIDSz is the number of bytes used to identify public-private key pairs and symmetric keys 
    KeyIDSz = 20

)

// KeyID identifies a public-private (asymmetric) key pair OR a private (symmetric) key.  
//    For asymmetric keys, it is defined as the right-most bytes of the public key.
//    For symmetric keys, it is randomly generated when the key bytes is generated.
type KeyID              [KeyIDSz]byte

// CommunityID identifies a PLAN community
type CommunityID        [IdentityAddrSz]byte

// IdentityPublicKey is the public key of a person, group, or sub community inside a PLAN community.
type IdentityPublicKey  [32]byte

// IdentityAddr the rightmost bytes of IdentityPublicKey
type IdentityAddr       [IdentityAddrSz]byte

// PDIEntrySig holds a final signature of a PDI entry, verifying the author's private key was used to sign the entry
type PDIEntrySig        []byte

// ChannelID identifies a specific PLAN channel where PDI entries are posted to.
type ChannelID          [20]byte



var (

	// RootAccessChannel is the community's root access-level channel, meaning this channel effectively
	//    specifies which community members are "community admins".  All other channels and access channels
	//    are ultimately controlled by the community members listed in this root channel.  This means
	//    the hierarchy of access channels is rooted in this channel.
	// Note that the parent access channel is set to itself by default.
	RootAccessChannel = ChannelID {
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
	MemberRegistryChannel = ChannelID {
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
	ChannelCatalogChannel = ChannelID {
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 3,
	}
)



// PDIEntryVerb is the top-level PDI signal that specifies one of the low-level, built-in purposes of a PDI entry.
type PDIEntryVerb       int32


const (

	// PDIEntryVerbPostAdminEntry inserts an channel administrative action (e.g. changing a channel property) -- requires channel ownership access. 
	PDIEntryVerbPostAdminEntry = 1

	// PDIEntryVerbPostEntry posts this entry body to the specified channel
	PDIEntryVerbPostEntry

	// PDIEntryVerbReplaceEntry replaces the cited entry with this one in its place
	PDIEntryVerbReplaceEntry
)

// PDIEntryInfo contains flags and info about this entry, in effect specifying which hash functions and crypto to apply.
type PDIEntryInfo [4]byte

// PDIEntryVers dictates the serialization format of this entry post
type PDIEntryVers byte

const (

	// PDIEntryVers1 default schema
	PDIEntryVers1 byte = 1 + iota
)

// Time specifies a second and accompanying nanosecond count.   63 bit second timstamps are used, ensuring that clockflippiug
//    won't occur until the year 292,471,210,648 CE.  I wonder if OSes will be still be made by for-profit orgs by that time.    
// Note: if a nanosecond precision is not available or n/a, then the best available precision should be used (or 0).  
type Time struct {
    UnixSecs            int64   `json:"unix"` // UnixSecs is the UTC in seconds elapsed since Jan 1, 1970.  This number can be zero or negative.
    NanoSecs            int32   `json:"nano"` // NanoSecs is the number of nanoseconds elapsed into .UnixSecs so the domain is [0,999999999]
}



const (

	// DistantFuture is a const used to express the "distant future" in unix time.
    DistantFuture       int64 = (1 << 63) - 1

	// DistantPast is a const used to express the "distant past" in unix time.
	DistantPast         int64 = -DistantFuture
)

// Now returns PLAN's standard time struct set to the time index of the present moment.
func Now() Time {
    t := time.Now()

    return Time{
        UnixSecs: t.Unix(),
        NanoSecs: int32( t.Nanosecond() ),
    }
}





/*
A community KeyID identifies a specific shared "community-global" symmetric key.
When a PLAN client starts a session with a pnode, the client sends the pnode her community-public keys.
PDIEntryCrypt.CommunityKeyID specifies which community key was used to encrypt PDIEntryCrypt.Header.
If/when an admin of a community issues a new community key,  each member is securely sent this new key via
the community key channel (where is key is asymmetrically sent to each member still "in" the community. 

*/

/*
PDIEntryCrypt is the public "wire" format for PDI entries.  It is what it sent to/from community data store implementations,
such as Ethereum and NEM.

A PDI entry has two encrypted segments, its header and body segment.  The PDIEntryCrypt.Header is encrypted using a community-public key,
specified by PDIEntryCrypt.CommunityKeyID.  This ensures non-community members (i.e. public snoops) can't see any entry params,
data, or even to what community channel the entry is being post to.  PDIEntryCrypt.Body is encrypted by the same community key as the header *or*
by the key specified via PDIEntryHeader.Author and PDIEntry.AccessChannelID (all via the client's Secure Key Interface).  In sum,
PDIEntry.AccessChannelID specifies a decryption flow through the user's private, compartmentalized keychain.

When a PLAN client starts a new session with a pnode, the client sends the community keys for the session.  This allows
pnode to process and decrypt incoming PDIEntryCrypt entries from the storage medium (e.g. Ethereum).  Otherwise, pnode
has no ability to decrypt PDIEntryCrypt.Header.  A pnode could be configured to keep the community keychain even when there
are no open client PLAN sessions so that incoming entries can be processed.  Otherwise, incoming entries won't be processed
from the lowest level PDI storage layer.  Both configurations are reasonable depending on security preferences.

When a community admin initiates a community-key "rekey event", the newly generated community key is securely and individually
"sent" to each community member via the community's public key transfer channel. The new community key is encrypted using each member's public key.
When a pnode is processing an entry that it does not have a community key for, it will check the community's public key channel
for an entry for the current client's public key, it will send the client the encrypted community key.  The client uses its SKI
to decrypt the payload into the new community key.  This key is added to the user's SKI keychain and is sent back to pnode.

When pnode sends a PLAN client is PDI entries, it sends PDIEntry.Header (via json) and PDIEntryCrypt.Body.  If the

Recall that the pnode client has no ability to decrypt PDIEntryCrypt.Body if PDIEntryHeader.AccessChannelID isn't set for
community-public permissions.  This is fine since only PLAN clients with a

And awaaaaay we go!
*/
type PDIEntryCrypt struct {
	Sig                 []byte              // Signature of PDIEntry.Hash (signed by PDIEntryHeader.Author)

	Info                PDIEntryInfo        // Entry type info, Allows PDIEntry accessors to apply the correct hash and crypto functions
	CommunityKeyID      KeyID

	HeaderCrypt         []byte              // Encrypted using a community key, referenced via .CommunityKeyID
	BodyCrypt           []byte              // Encrypted using a community key or a user private key (based on PDIEntryHeader.AccessChannelID)

}

/*
// PDIEntry is wha
type PDIEntry struct {
	HeaderBuf           []byte              // Serialized representation of PDIEntry.Header  (decrypted form PDIEntryCrypt.Header)
	Header              *PDIEntryHeader

	BodyBuf             []byte              // Serialized representation of PDIEntry.Body (decrypted from PDIEntryCrypt.Body)
	Body                *PDIEntryBody
}
*/

// PDIEntryHeader is a container for community-public info about this channel entry.PDIEntryHeader
type PDIEntryHeader struct {
    Time                Time                // Timestamp when .Author sealed this entry.
	Verb                PDIEntryVerb        // PDI command/verb
    ChannelID           ChannelID           // The channel id this entry is posted to.
    ChannelRev          int32               // Revision numnber of this channel this entry is targeting
    AuthorID            IdentityAddr        // Creator of this entry (and signer of .Sig)
    AuthorIdentityRev   int32               // Specifies which rev of the author's public key was used for ecryption (or 0 if n/a)
	AccessChannelID     ChannelID            // Specifies the permissions channel (an access control implementation) this entry was encrypted with
	AccessChannelRev    int32               // Specifies what identifying major rev of the access channel was in effect when this entry was authored
	AuxHeader           http.Header         // Any auxillary header entries that apply to this PDI entry -- always UTF8
}

// PDIEntryBody is the decrypted and de-serialized form of PDIEntryCrypt.Body and an abstract data container.
type PDIEntryBody struct {
	BodyParts           []PDIBodyPart       // Zero or more data sections -- e.g. .parts[0] is a serialized json param list and .parts[1] is a blob of rich text (rtf).
}


// PDIBodyPart is one of one or more sequential parts of a PDIEntryBody.PDIBodyPart.
type PDIBodyPart struct {
	Header              http.Header         // Any auxillary header entries that apply to this PDI entry -- always UTF8
	Content             []byte              // Arbitrary client binary data conforming to .Headers
}

const (

    // PDIContentCodecHeaderName is the standard header field name used in PDIBodyPart.Headers used to describe PDIBodyPart.Content.
    // For info and background: https://github.com/multiformats/multicodec
    PDIContentCodecHeaderName = "Multistream"
)


// When a new PDIEntry arrives off the wire, .Sig is set, .Hash == nil, .dataCrypt is set, .Body == nil
//    1) .dataCrypt is hashed via  PDIEntrySchema.BodyHasher
//    2) .Sig is verified based on the hash calculated from (1)
//    3) Use .AccessChannelID to specify a decryption flow through PLAN's Secure Key Interface (SKI).
//         - If the local user pubkey does not have "read" access, then that means there's a db corresponding to that access list that contains a list of all entries
//           that have been encountered (and can't be unlocked).  In this case, referencing info for this entry is appended to this db and entry is not
//           processed further.  If/when the CommunityRepo sees an access control list get unlocked for the current logged in user, it will
//         -
// , then the entry data can only be stored until the user is granted access.
//    2) Compute Hash of
//    2) Decrypt .dataCrypt into .dataRLP (a generic blob)
//    3) De-serialize this output blob (instantiate .Body).


// ChannelProperties specifies req'd params for all channels and are community-public.
// Note that some params are immutable once they are set in the channel genesis block (e.g. IsAccessChannel and EntriesAreFinal)
type ChannelProperties struct {

	// Who issued this set of channel properties
	Author                  IdentityAddr            `json:"author"`

	// Does this channel conform to the requirements needed so that this channel can be used to control access for another channel.
	// An access channel's purpose is to:
	//     (a) distribute private channel keys to members (by encrypting a private channel key using a member's public key)
	//     (b) publish write and own privs to others (only permitted by owners)
	IsAccessChannel         bool                    `json:"isAccessChannel"`

	// Are entries in this channel allowed be revoked or superceded?
	EntriesAreFinal         bool                    `json:"entriesFinal"`

	// This channel's ID
	ChannelID               ChannelID               `json:"chID"`

	// Specifies an owning access channel that asserts domain over this channel
	OwningAccessChannelID   ChannelID               `json:"acID"`

	// Specifies which major revision number of the owning access channel was in effect when this entry was authored.
	// In general, a pnode won't insert/approve of a PDI entry until/unless the access channel revisions match.
	OwningAccessChannelRev  uint32                  `json:"acRev"`

	// Complete set of channel params
	Params                  map[string]interface{}  `json:"params"`
}

/*
type ChannelInfo struct {

    // Specifies when this channel was created
    TimeCreated             Time                    `json:"inEffect"`

    // <<multistream>-inspired description of this channel's contents.  This allows the PLAN client to accurately process, interpret, and handle entry
    //    entry data blobs (PDIEntry.Body -- i.e. PDIEntryBody).  example:  "/plan/talk/v2"
    Protocol                string                  `json:"protocol"`

    // What is the title of this channel presented to participants?
    ChannelTitle            string                  `json:"chTitle"`

    // What is the description of this channel presented to participants?
    ChannelDesc             string                  `json:"chDesc"`

}
*/




///////////////////////////////////////////
//  Common Utility & Conversion Helpers  //
///////////////////////////////////////////


// Assert is PLAN's easy assert
func Assert( inCond bool, inFormat string, inArgs ...interface{} ) {

	if ! inCond {
		panic(fmt.Sprintf(inFormat, inArgs))
	}
}

// ComputeHash hashes all fields of PDIEntryCrypt (except .Sig)
func (inEntry *PDIEntryCrypt) ComputeHash() []byte {

	hw := sha3.NewKeccak256()
	hw.Write(inEntry.Info[:])
	hw.Write(inEntry.CommunityKeyID[:])
	hw.Write(inEntry.HeaderCrypt)
	hw.Write(inEntry.BodyCrypt)
    
    return hw.Sum( nil )

    /*
    overhang := len(hash)-PDIEntryHashSz
	if overhang > 0 {
		hash = hash[overhang:]
	}
    copy( outHash[PDIEntryHashSz-len(hash):], hash)*/

}

/*
// CopyFrom is a convenience function to copy from a byte slice.
func (sig *PDIEntrySig) CopyFrom( inSig []byte ) {
	copy( sig[:], inSig[:len(sig)] )
}
*/


// NewPDIEntrySig is a convenience function to create a PDIEntrySig from an existing sig
func NewPDIEntrySig( inBytes []byte ) PDIEntrySig {
	sig := PDIEntrySig{}
	copy(sig[:], inBytes[:64])
	return sig
}

/*
// ToArray is a convenience function to copy to a byte array.
func (ckey *CommunityKey) ToArray() *[32]byte  {
    var arr [len(ckey)]byte

    copy( arr[:], ckey[:len(ckey)])
    
	return &arr
}*/

// NewIdentityPublicKey is a convenience function to create a IdentityPublicKey from an existing key
func NewIdentityPublicKey(inBytes *[32]byte) IdentityPublicKey {
	k := IdentityPublicKey(*inBytes)
	return k
}

// Assign sets this CommunityID from the given buffer
func (cid *CommunityID) Assign(in []byte) {
    copy(cid[:], in[:IdentityAddrSz])
}

// Assign sets this CommunityID from the given buffer
func (cid *IdentityAddr) Assign(in []byte) {
    copy(cid[:], in[:IdentityAddrSz])
}


// Assign sets this IdentityPublicKey from the given buffer
func (ipk *IdentityPublicKey) Assign( inArray *[32]byte ) {
	copy(ipk[:], inArray[:32])
}

// ToArray is a convenience function to copy to a byte array.
func (ipk *IdentityPublicKey) ToArray() *[32]byte  {
    var arr [32]byte

    copy( arr[:], ipk[:32] )
    
	return &arr
}


