package plan

// BlockSearchScope specifies what parts of a Block to search for matches.
// The GetBlock*() calls below that don't accept a BlockSearchScope parameter implicitly use:
//      SearchBlocksSelf + SearchBlocksShallow
type BlockSearchScope int

// CodecCodeForBlock signals that the associated buffer was created from plan.Block.Marshal()
const CodecCodeForBlock = 0x0201

// CodecCodeForEntryCrypt signals that the associated buffer was created from pdi.EntryCrypt.Marshal()
const CodecCodeForEntryCrypt = 0x0202

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

// GetBlocksWithCodec returns all Blocks with a matching codec string (SearchBlocksSelf + SearchBlocksShallow)
func (block *Block) GetBlocksWithCodec(inCodec string, inCodecCode uint32) []*Block {
	var matches []*Block

	if len(inCodec) > 0 {

		if (inCodecCode != 0 && inCodecCode == block.CodecCode) || inCodec == block.Codec {
			matches = append(matches, block)
		}

		for _, sub := range block.Subs {
			if (inCodecCode != 0 && inCodecCode == sub.CodecCode) || sub.Codec == inCodec {
				matches = append(matches, sub)
			}
		}

	} else {

		if inCodecCode != 0 && inCodecCode == block.CodecCode {
			matches = append(matches, block)
		}

		for _, sub := range block.Subs {
			if inCodecCode != 0 && inCodecCode == sub.CodecCode {
				matches = append(matches, sub)
			}
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

// GetBlockWithCodec returns the first-appearing Block with a matching codec string (self and shallow search only)
func (block *Block) GetBlockWithCodec(inCodec string, inCodecCode uint32) *Block {

	checkCodecStr := len(inCodec) > 0

	// Start w/ the "self" block
	b := block

	N := len(block.Subs)
	for i := -1; i < N; i++ {
		if i >= 0 {
			b = block.Subs[i]
		}

		if (inCodecCode != 0 && inCodecCode == b.CodecCode) || (checkCodecStr && inCodec == b.Codec) {
			return b
		}
	}

	return nil
}

// GetContentWithLabel returns the content of the first-appearing Block with a matching label/name
func (block *Block) GetContentWithLabel(inLabel string) []byte {
	blk := block.GetBlockWithLabel(inLabel)

	if blk != nil {
		return blk.Content
	}

	return nil
}

// GetContentWithCodec returns the content of the first-appearing Block with a matching label/name
func (block *Block) GetContentWithCodec(inCodec string, inCodecCode uint32) []byte {
	blk := block.GetBlockWithCodec(inCodec, inCodecCode)

	if blk != nil {
		return blk.Content
	}

	return nil
}

// AddBlock appends the given block to this block's list of sub blocks
func (block *Block) AddBlock(inBlock *Block) {
	block.Subs = append(block.Subs, inBlock)
}

// AddContentWithLabel appends a new block with the given label and content
func (block *Block) AddContentWithLabel(inContent []byte, inLabel string) {
	block.AddBlock(&Block{
		Label:   inLabel,
		Content: inContent,
	})
}

// AddContentWithCodec appends a new block with the given content buf an accompanying multicodec path
func (block *Block) AddContentWithCodec(inContent []byte, inCodec string) {
	block.AddBlock(&Block{
		Codec:   inCodec,
		Content: inContent,
	})
}

// InvokeBlocksWithCodec performs a self and shallow search for matching Blocks.
// If an error is encountered, iteration stops and the error is returned.
func (block *Block) InvokeBlocksWithCodec(
	inCodec string,
	inCodecCode uint32,
	inCallback func(inMatch *Block) error,
) error {

	checkCodecStr := len(inCodec) > 0

	var err error

	// Start w/ the "self" block
	b := block

	N := len(block.Subs)
	for i := -1; i < N && err == nil; i++ {
		if i >= 0 {
			b = block.Subs[i]
		}

		if (inCodecCode != 0 && inCodecCode == b.CodecCode) || (checkCodecStr && inCodec == b.Codec) {
			err = inCallback(b)
		}
	}

	return err
}
