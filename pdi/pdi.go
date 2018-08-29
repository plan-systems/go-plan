

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
// IOAccessFlags is used to express read, write, read-write, or no access
type IOAccessFlags int32
const (

    // ReadAccess means data will be read
    ReadAccess          IOAccessFlags = 0x01

    // AppendAccess means data will be appended
    AppendAccess        IOAccessFlags = 0x02

    // WriteAccess means data will be written
    WriteAccess         IOAccessFlags = 0x04
)


var (
    ChannelAccessForEntryOp = map[EntryOp][2]IOAccessFlags {
        // EntryOp                      Channel         AccessChannel
        EntryOp_POST_NEW_CONTENT:       {AppendAccess,  ReadAccess},
        EntryOp_UPDATE_CHANNEL_INFO:    {AppendAccess,  ReadAccess},
        EntryOp_REMOVE_ENTRIES:         {AppendAccess,  ReadAccess},
        EntryOp_SUPERCEDE_ENTRY:        {AppendAccess,  ReadAccess},
        EntryOp_UPDATE_ACCESS_GRANTS:   {AppendAccess,  AppendAccess},
        EntryOp_EDIT_CHANNEL_EPOCH:     {AppendAccess,  AppendAccess},
    }
)
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



// GetBlockWithLabel returns the first-appearing Block with the matching block label
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

// GetBlockWithCodec returns the // GetBlockWithLabel returns the first-appearing Block with the matching codec name
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




// GetContentWithLabel returns the content of the first-appearing BodyPart with the matching part and codec name
func (block* Block) GetContentWithLabel(inLabel string) []byte {
    blk := block.GetBlockWithLabel(inLabel)

    if blk != nil {
        return blk.Content
    }

    return nil
}



// GetContentWithCodec returns the content of the first-appearing BodyPart with the matching codec name
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

// AddContentWithCodec appends a new block with the given content buf an accompanying mulicodec
func (block* Block) AddContentWithCodec(inContent []byte, inCodec string) {
    block.Subs = append(
        block.Subs, 
        &Block {
            ContentCodec:inCodec,
            Content:inContent,
        },
    )
}


