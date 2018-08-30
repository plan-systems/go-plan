
/**********************************************************************************************************************

P           Purposeful
P  L        Logistics
P  L  A     Architecture
P  L  A  N  Network

P  L  A  N
u  o  r  e
r  g  c  t
p  i  h  w
o  s  i  o
s  t  t  r
e  i  e  k
f  c  c  
u  s  t
l     e


Welcome to PLAN!  go-plan is the benchmark implementation of PLAN's p2p client-serving node.

PLAN is a secure, all-in-one communications and logistics planning tool for individuals and communities.  It
is intended to be an instrument of productivity, vision, and self-reliance.

All the parts of PLAN are free and open-source (GPLv3), and each component and layer is intended to be "pluggable" 
or otherwise an embodiment of an open-protocol.  The design principles of PLAN are similar and consistent with
Tim Berners-Lee's design and implementation of http. 

Universe willing, may PLAN give organizations and communities with little or no resources
the ability to communicate and self-organize intuitively and reliably. I see PLAN supporting her users, 
the members of the PLAN Foundation, and myself such that I could not wish for more.  

~ Proto, Summer 2018  */

package plan


import (
    "fmt"
    "time"

)


// DataHandler is a deferred data handler function
type DataHandler            func(inParam []byte, inErr *Perror)

// Action is a deferred generic handler. 
type Action                 func(inParam interface{}, inErr *Perror)




const (

	// IdentityAddrSz is the number of bytes PLAN uses for a public addresses.  It's the right-most bytes of any longer public key.
    // Background on probability of address collision for 20 bytes (160 bits) http://preshing.com/20110504/hash-collision-probabilities/
    // Why shouldn't this be smaller or larger?  Now this is becoming an existential question!  What's the human perceptual difference between,
    //     say, 1/2^128, 1/2^160, and 1/2^192?  We're living it and finding out!  2^192 outta be enough for anybody.
    IdentityAddrSz      = 24
    
    // KeyIDSz is the number of bytes PLAN uses to identify a key entry on a keyring 
    KeyIDSz             = 24

    // ChannelIDSz specifies the size of ChannelID
    ChannelIDSz         = 20

    // MemberIDSz specifies the size of MemberID
    MemberIDSz          = 16

)

// KeyID identifies a public-private (asymmetric) key pair OR a private (symmetric) key.  
//    For asymmetric keys, it is defined as the right-most bytes of the public key.
//    For symmetric keys, it is randomly generated when the key bytes is generated.
type KeyID              [KeyIDSz]byte

// CommunityID identifies a PLAN community
type CommunityID        [IdentityAddrSz]byte

// IdentityPublicKey is the public key of a person, group, or sub community inside a PLAN community.
type IdentityPublicKey  []byte

// IdentityAddr the rightmost bytes of IdentityPublicKey
type IdentityAddr       [IdentityAddrSz]byte

// PDIEntrySig holds a final signature of a PDI entry, verifying the author's private key was used to sign the entry
type PDIEntrySig        []byte

// ChannelID identifies a specific PLAN channel where PDI entries are posted to.
type ChannelID          [ChannelIDSz]byte

// MemberID identifies a member of a given community and is generared when a member is initially added to the community and never changes 
type MemberID           [MemberIDSz]byte

// MemberEpoch changes each time a member creates a new set of public keys.  The community's MemberRegistryChannel allows 
//    any member to lookup public keys for a member for each crypto rev they've ever done, allows community members to:
//    (1) send private messages to a given member
//    (2) verify sigs on anything to ensure that they are authentic
type MemberEpoch        uint64


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
	//    each community member's member ID, latest public keys, and member info (e.g. home ChannelID).  This allows each of the
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





// Time specifies a second and accompanying nanosecond count.   63 bit second timstamps are used, ensuring that clockflipping
//     won't occur until the year 292,471,210,648 CE.  I wonder for-profit orgs will still dominate the OS space.
// Note: if a nanosecond precision is not available or n/a, then the best available precision should be used (or 0).  
type Time struct {
    UnixSecs            int64   `json:"unix"`   // UnixSecs is the UTC in seconds elapsed since Jan 1, 1970.  This number can be zero or negative.
    NanoSecs            int32   `json:"nano"`   // NanoSecs is the number of nanoseconds elapsed into .UnixSecs so the domain is [0,999999999]
}



// Now returns PLAN's standard time struct set to the time index of the present moment.
func Now() Time {
    t := time.Now()

    return Time {
        UnixSecs: t.Unix(),
        NanoSecs: int32( t.Nanosecond() ),
    }
}


const (

	// DistantFuture is a const used to express the "distant future" in unix time.
    DistantFuture       int64 = (1 << 63) - 1

	// DistantPast is a const used to express the "distant past" in unix time.
	DistantPast         int64 = -DistantFuture
)






///////////////////////////////////////////
//  Common Utility & Conversion Helpers  //
///////////////////////////////////////////


// Assert is PLAN's easy assert
func Assert( inCond bool, inFormat string, inArgs ...interface{} ) {

	if ! inCond {
		panic(fmt.Sprintf(inFormat, inArgs))
	}
}



// AssignFrom sets this CommunityID from the given buffer
func (cid *CommunityID) AssignFrom(in []byte) {
    copy(cid[:], in[len(in) - IdentityAddrSz:])
}

// AssignFrom sets this CommunityID from the given buffer
func (cid *IdentityAddr) AssignFrom(in []byte) {
    copy(cid[:], in[len(in) - IdentityAddrSz:])
}

// AssignFrom sets this CommunityID from the given buffer
func (kid *KeyID) AssignFrom(in []byte) {
    copy(kid[:], in[len(in) - KeyIDSz:])
}

// AssignFrom sets this ChannelID from the given buffer
func (cid *ChannelID) AssignFrom(in []byte) {
    copy(cid[:], in[len(in) - ChannelIDSz:])
}

