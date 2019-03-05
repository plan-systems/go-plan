/**********************************************************************************************************************

         P urposeful
         L ogistics
         A rchitecture
P  L  A  N etwork

May PLAN support her users, the members of the PLAN Foundation, and myself so that I could not wish for more.

~ proto, summer 2018  */

package plan

import (
	"time"
    "os"
    "encoding/base64"
)

// DataHandler is a deferred data handler function
type DataHandler func(inParam []byte, inErr error)

// Action is a deferred generic handler.
type Action func(inParam interface{}, inErr error)

const (

	// CommunityIDSz is the number of bytes PLAN uses for a community ID.
	// Background on probability of hash collision: http://preshing.com/20110504/hash-collision-probabilities/
	// Should this be smaller or larger?  Philosophically, this value expresses the size of the hash universe,
	//    where nodes can "safely" generate hashnames alongside peers.  2^192 outta be enough for anybody.
	CommunityIDSz = 24

	// KeyIDSz is the number of bytes used to identify a key entry on a PLAN keyring.
	// It's "modest-sized" since a newly generated key must pass collision checks before it's put into use.
	KeyIDSz = 16

	// ChannelIDSz specifies the byte size of a channel ID
    ChannelIDSz = 16

	// MemberAliasMaxLen is the max UTF8 string length a community member can use for their member alias
	MemberAliasMaxLen = 127

    // Base64CharSet is the base 64 char set used in PLAN, chosen such that 0 maps to '0' and is monotonic increasing (which can be sorted).
    Base64CharSet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"

)

// CommunityID identifies a PLAN community and is randomly generated during its genesis.
type CommunityID [CommunityIDSz]byte

// MemberID identifies a member within a given community and never changes -- even when a member initiates
//    a new "epoch" so their crypto can be regenerated.
// Member IDs are considered collision-proof since inside a community, they must be cleared through
//    the community's new member registration process (which will reject a collision).
type MemberID uint64

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

// ChannelID identifies a specific PLAN channel where PDI entries are posted to (for a given a community ID).
type ChannelID [ChannelIDSz]byte

// KeyID identifies a cryptographic key (for a given a community ID).
//    For asymmetric keys, it is defined as the right-most bytes of the public key.
//    For symmetric keys, it is randomly generated when the key is generated.
type KeyID [KeyIDSz]byte

// MemberEpoch changes each time a member creates a new set of public keys.  The community's MemberRegistryChannel allows
//    any member to lookup public keys for a member for each crypto rev they've ever done, allows community members to:
//    (1) send private messages to a given member
//    (2) verify sigs on anything to ensure that they are authentic
type MemberEpoch uint64

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
		0, 0, 0, 1,
	}	
    
    // MemberRegistryChannel is the community's master (community-public) member registry.  Each entry specifies a
	//    each community member's member ID, latest public keys, and member info (e.g. home ChannelID).  This allows each of the
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
		0, 0, 0, 3,
	}

    // DefaultFileMode is used to express the default mode of file creation.
    DefaultFileMode = os.FileMode(0775)

    // Base64 encodes/decodes binary strings.
    Base64 = base64.NewEncoding(Base64CharSet).WithPadding(base64.NoPadding)

)

// Time specifies a second and accompanying nanosecond count.   63 bit second timstamps are used, ensuring that clockflipping
//     won't occur until the year 292,471,210,648 CE.  I wonder for-profit orgs will still dominate the OS space.
// Note: if a nanosecond precision is not available or n/a, then the best available precision should be used (or 0).
type Time struct {
	UnixSecs int64  `json:"unix"` // UnixSecs is the UTC in seconds elapsed since Jan 1, 1970.  This number can be zero or negative.
	FracSecs uint16 `json:"frac"` // FracSecs is 16 bit fraction from 0 to 0xFFFF
}

// Now returns PLAN's standard time struct set to the time index of the present moment.
func Now() Time {
	t := time.Now()

	return Time{
		UnixSecs: t.Unix(),
		FracSecs: uint16((2199 * (uint32(t.Nanosecond()) >> 10)) >> 15),
	}
}

const (

	// DistantFuture is a const used to express the "distant future" in unix time.
	DistantFuture int64 = (1 << 63) - 1

	// DistantPast is a const used to express the "distant past" in unix time.
	DistantPast int64 = -DistantFuture
)

