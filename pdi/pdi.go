

package pdi


import (
    "github.com/ethereum/go-ethereum/crypto/sha3"
    "github.com/plan-tools/go-plan/plan"
)




const (
    entryVersionMask = 0xFF

	// EntryVersion1 default schema
    EntryVersion1 int = 1 + iota

)

/*

var (
    EntryOpInfo = map[EntryOp]ChannelAccess{
        // EntryOp                      MinAuthorAccess
        EntryOp_POST_NEW_CONTENT:       ChannelAccess_READWRITE_ACCESS,
        EntryOp_EDIT_CHANNEL_INFO:    ChannelAccess_MODERATOR_ACCESS,
        EntryOp_REMOVE_ENTRIES:         ChannelAccess_MODERATOR_ACCESS
        EntryOp_SUPERCEDE_ENTRY:        ChannelAccess_MODERATOR_ACCESS,

        // Only sent to access channels (target channel is an access channel)
        EntryOp_UPDATE_ACCESS_GRANTS:   {WriteAccess,           AppendAccess},
        EntryOp_NEW_CHANNEL_EPOCH:      {WriteAccess,           AppendAccess},
    }

)

type ChannelStoreFlags int32
const (

    // ReadAccess means data will be read
    ReadAccess      ChannelStoreFlags = 0x01

    // WriteAccess means data will be written
    WriteAccess     ChannelStoreFlags = 0x02

    // IsAccessChannel means the given the channel is expected to be an access channel
    IsAccessChannel ChannelStoreFlags = 0x10
)
*/
/*
// IOAccessFlags is used to express read, write, read-write, or no access

um ChannelAccess {

    // Not used
    NO_ACCESS                   = 0;

    // Has crypto to decrypt entries from the given channel
    READ_ACCESS                 = 1;

    // Permitted to author: POST_NEW_CONTENT, REMOVE_ENTRIES, SUPERCEDE_ENTRY, 
    READWRITE_ACCESS            = 2;

    // Permitted to author: POST_NEW_CONTENT, REMOVE_ENTRIES, SUPERCEDE_ENTRY, UPDATE_CHANNEL_INFO, UPDATE_ACCESS
    MODERATOR_ACCESS            = 3;

    // Same as MODERATOR_ACCESS plus can grant others MODERATOR_ACCESS and can issue UPDATE_ACCESS_GRANTS
    SUPER_MODERATOR_ACCESS      = 4;

    // Same as SUPER_MODERATOR_ACCESS plus can grant others SUPER_MODERATOR_ACCESS and can author entry type NEW_CHANNEL_EPOCH
    ADMIN_ACCESS                = 5;


/ Appends this content entry to the specified channel.
    POST_NEW_CONTENT            = 0;

    // This entry modifies one or more of a given channel's meta fields (e.g. channel description, icon, etc).
    // A ChannelInfo snapshot/composite can be reconstructed by sequentially applying every UPDATE_CHANNEL_INFO 
    //    change to the previous ChannelInfo composite up to a given point in time.
    // Only channel admins or moderators are permitted to originate this type of entry.
    UPDATE_CHANNEL_INFO         = 1;

    // This entry's body lists one or more channel entries to effectively mark as removed/invisible.
    REMOVE_ENTRIES              = 2;

    // This entry's body should effectively replace a specified previous entry
    SUPERCEDE_ENTRY             = 3;

    // Adds or removes access to given member IDs.  Notes:
    //   - This entry type is only valid for use channels that are access control channels.
    //   - Members with MODERATOR_ACCESS can only grant/revoke READ_ACCESS and READWRITE_ACCESS.
    //   - Members with ADMIN_ACCESS can grant/revoke up to and including SUPER_MODERATOR_ACCESS.
    //   - ADMIN_ACCESS can ONLY be granted by members with ADMIN_ACCESS in the parent access control channel.
    //   - In some cases, this entry type MAY result a new channel epoch to be initiated (this is because
    //     a private channel must issue and distribute a new channel encryption key in order to effectively 
    //     remove access to members that are longer have channel access).
    UPDATE_ACCESS_GRANTS        = 4;


    // This entry initiates a new channel epoch, incorporating one or more changes to the channel's current epoch fields (ChannelEpoch).
    // Only channel admins are allowed to originate this type of entry.
    NEW_CHANNEL_EPOCH          = 5;


    // Network-level 
    SECURITY_ALERT              = 9;
}

*/




// GetEntryVersion returns the version of this entry (should match EntryVersion1)
func (entry *EntryCrypt) GetEntryVersion() int {
    return int(entry.CryptInfo & entryVersionMask)
}


// GetCommunityKeyID is a convenience function that returns EntryCrypt.CommunityKeyId as a KeyID
func (entry *EntryCrypt) GetCommunityKeyID() plan.KeyID {
    var keyID plan.KeyID
    keyID.AssignFrom(entry.CommunityKeyId)
    return keyID
}



// ComputeHash hashes all fields of psi.EntryCrypt (except .EntrySig)
func (entry *EntryCrypt) ComputeHash() []byte {

    hw := sha3.NewKeccak256()

    scrap := make([]byte, 16)

    pos := 0
    pos = encodeVarintPdi(scrap, pos, entry.CryptInfo)

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityKeyId[:])
	hw.Write(entry.HeaderCrypt)
	hw.Write(entry.BodyCrypt)
    
    return hw.Sum( nil )

}


// BlockSearchScope specifies what parts of a Block to search for matches.
// The GetBlock*() calls below that don't accept a BlockSearchScope parameter implicitly use: 
//      SearchBlocksSelf + SearchBlocksShallow
type BlockSearchScope int
const (

    // SearchBlocksSelf means the root block is analyzed as a possible match.
    SearchBlocksSelf BlockSearchScope = 1 + iota

    // SearchBlocksShallow searches the "subs" part of the given target but no more
    SearchBlocksShallow

    // SearchBlocksDepthFirst searches into each sub-Block recursively.
    SearchBlocksDepthFirst

)




// GetBlocksWithLabel returns all Blocks with a matching block label (SearchBlocksSelf + SearchBlocksShallow)
func (block *Block) GetBlocksWithLabel(inLabel string) []*Block {
    var matches []*Block

    if inLabel == block.Label {
        matches = append(matches, block)
    }

    for _, sub := range block.Subs {
        if sub.Label == inLabel {
            matches = append(matches, sub)
        }
    }

    return matches
}


// GetBlockWithLabel returns the first-appearing Block with a matching block label -- see GetBlocksWithLabel()
func (block *Block) GetBlockWithLabel(inLabel string) *Block {

    if inLabel == block.Label {
        return block
    }

    for _, sub := range block.Subs {
        if sub.Label == inLabel {
            return sub
        }
    }

    return nil
}


// GetBlocksWithCodec returns all Blocks with a matching codec string (SearchBlocksSelf + SearchBlocksShallow)
func (block *Block) GetBlocksWithCodec(inCodec string) []*Block {
    var matches []*Block

    if inCodec == block.ContentCodec {
        matches = append(matches, block)
    }

    for _, sub := range block.Subs {
        if sub.ContentCodec == inCodec {
            matches = append(matches, sub)
        }
    }

    return matches
}



// GetBlockWithCodec returns the first-appearing Block with a matching codec string
func (block *Block) GetBlockWithCodec(inCodec string) *Block {

    if inCodec == block.ContentCodec {
        return block
    }

    for _, sub := range block.Subs {
        if sub.ContentCodec == inCodec {
            return sub
        }
    }

    return nil
}





// GetContentWithLabel returns the content of the first-appearing Block with a matching label/name
func (block* Block) GetContentWithLabel(inLabel string) []byte {
    blk := block.GetBlockWithLabel(inLabel)

    if blk != nil {
        return blk.Content
    }

    return nil
}



// GetContentWithCodec returns the content of the first-appearing Block with a matching label/name
func (block* Block) GetContentWithCodec(inCodec string) []byte {
    blk := block.GetBlockWithCodec(inCodec)

    if blk != nil {
        return blk.Content
    }

    return nil
}

// AppendBlock appends the given block to this block's list of sub blocks
func (block* Block) AppendBlock(inBlock *Block) {
    block.Subs = append(block.Subs, inBlock)
}


// AddContentWithLabel appends a new block with the given label and content
func (block* Block) AddContentWithLabel(inContent []byte, inLabel string) {
    block.Subs = append(
        block.Subs, 
        &Block {
            Label:inLabel,
            Content:inContent,
        },
    )
}

// AddContentWithCodec appends a new block with the given content buf an accompanying multicodec path
func (block* Block) AddContentWithCodec(inContent []byte, inCodec string) {
    block.Subs = append(
        block.Subs, 
        &Block {
            ContentCodec:inCodec,
            Content:inContent,
        },
    )
}


