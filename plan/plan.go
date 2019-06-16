/**********************************************************************************************************************

         P urposeful
         L ogistics
         A rchitecture
P  L  A  N etwork

May PLAN support her users, the members of PLAN Systems, and myself so that I could not wish for more.  ~ Drew  */

package plan

import (
	"encoding/base64"
	"time"
)

const (

	// CommunityIDSz is the number of bytes used to to identify a PLAN community ID.
	// Background on probability of hash collision: http://preshing.com/20110504/hash-collision-probabilities/
	// Should this be smaller or larger?  Philosophically, this value expresses the size of the hash universe,
	//    where nodes can "safely" generate hashnames alongside peers.  2^192 outta be enough for anybody.
	CommunityIDSz = 24

	// SymmetricPubKeySz is the number of bytes used to identify a symmetric key entry on a PLAN keyring.
	// It's "modest-sized" since a newly generated keys must pass collision checks before being put into use.
	SymmetricPubKeySz = 16

	// WorkstationIDSz is the number of bytes used to to identify a PLAN workstation ID.
	WorkstationIDSz = 16

	// ChIDSz specifies the byte size of a PLAN channel ID (and is the right-most bytes of a TID.
	ChIDSz = 18

	// TIDSz is the number of bytes for a TID ("time ID")
	// Having 20 hash bytes is as strong as Ethereum and Bitcoin's address system.
	//
	// Byte layout is designed so that TIDs are sortable by an embedded timestamp:
	//    0:6   - Standard UTC timestamp in unix seconds (BE)
	//    6:8   - Timestamp fraction (BE)
	//    8:27  - Signature/hash bytes
	TIDSz = 27

	// MemberIDSz is the byte size of a MemberID
	MemberIDSz = 4

	// MemberAliasMaxLen is the max UTF8 string length a community member can use for their member alias
	MemberAliasMaxLen = 127

	// Base64pCharSet is the base 64 char set used in PLAN, chosen such that 0 maps to '0' and is monotonic increasing (which can be sorted).
	Base64pCharSet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"
)

// CommunityID identifies a PLAN community and is randomly generated during its genesis.
type CommunityID [CommunityIDSz]byte

// MemberID identifies a member within a given community and never changes -- even when a member initiates
//    a new "epoch" so their crypto can be regenerated.
// Member IDs are considered collision-proof since inside a community, they must be cleared through
//    the community's new member registration process (which will reject a collision).
type MemberID uint32

// StorageID identifies a storage instance within a given community.
//
// When a storage network/provider is created to host a given community, it is identified from other (previous)
// instances by ensuring that the newly assigned StorageID is unique in community's history,
// something trivially done by the person(s) leading the storage switchover.  In the lifetime of a community, there would
// only need to be a new StorageID generated when the community moved to a new storage provider/network.
type StorageID uint16

// MemberAlias is a self-given community member name and is how they are seen by humans in the community,
//    making it a convenience tool for humans to easily refer to other members.
// A member can change their MemberAlias at any time (though there may be reasonable restrictions in place).
// Note: a MemberID is generated from the right-most bytes of the SHA256 hash of the community ID concatenated
//    with the member's first chosen alias (or an alternatively entered "member ID generation phrase").  This
//    scheme makes the member ID recoverable from human memory, even if there is no network access.
type MemberAlias string

// TID identifies a specific PLAN channel, channel entry, or anything else labeled via plan.GenerateIID()
type TID []byte

// TIDBlob is a fixed-length buffer that contains a TID
type TIDBlob [TIDSz]byte

// ChID identifies a PLAN channel.
type ChID []byte

// ChIDBlob is a fixed-length buffer that contains a ChID
type ChIDBlob [ChIDSz]byte

var (
	// Base64p encodes/decodes binary strings.
	Base64p = base64.NewEncoding(Base64pCharSet).WithPadding(base64.NoPadding)

	// GenesisMemberID is the genesis member ID
	GenesisMemberID = uint32(1)
)

// TimeFS is the UTC in 1/1<<16 seconds elapsed since Jan 1, 1970 UTC ("FS" = fractional seconds)
//
// Shifting this right 16 bits will yield stanard Unix time.
// This means there are 47 bits dedicated for seconds, implying max timestamp of 4.4 million years.
//
// Note: if a precision deeper than one seconds is not available or n/a, then the best available precision should be used (or 0).
type TimeFS int64

// NowFS returns the current time (a standard unix UTC timestamp in 1/1<<16 seconds)
func NowFS() TimeFS {
	t := time.Now()

	timeFS := t.Unix() << 16
	frac := uint16((2199 * (uint32(t.Nanosecond()) >> 10)) >> 15)
	return TimeFS(timeFS | int64(frac))
}

// Now returns the current time as a standard unix UTC timestamp.
func Now() int64 {
	return time.Now().Unix()
}

const (

	// TimeFSMax is the largest possible TimeFS value
	TimeFSMax = TimeFS(DistantFuture)

	// DistantFuture is a const used to express the "distant future" in unix time.
	DistantFuture int64 = (1 << 63) - 1

	// DistantPast is a const used to express the "distant past" in unix time.
	DistantPast int64 = -DistantFuture
)

// NilTID is a reserved TID that denotes a void/nil/zero value of a TID
var NilTID = TIDBlob{
	0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0,
}

