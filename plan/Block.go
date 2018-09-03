package plan

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

	if inCodec == block.Codec {
		matches = append(matches, block)
	}

	for _, sub := range block.Subs {
		if sub.Codec == inCodec {
			matches = append(matches, sub)
		}
	}

	return matches
}

// GetBlockWithCodec returns the first-appearing Block with a matching codec string
func (block *Block) GetBlockWithCodec(inCodec string) *Block {

	if inCodec == block.Codec {
		return block
	}

	for _, sub := range block.Subs {
		if sub.Codec == inCodec {
			return sub
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
func (block *Block) GetContentWithCodec(inCodec string) []byte {
	blk := block.GetBlockWithCodec(inCodec)

	if blk != nil {
		return blk.Content
	}

	return nil
}

// AppendBlock appends the given block to this block's list of sub blocks
func (block *Block) AppendBlock(inBlock *Block) {
	block.Subs = append(block.Subs, inBlock)
}

// AddContentWithLabel appends a new block with the given label and content
func (block *Block) AddContentWithLabel(inContent []byte, inLabel string) {
	block.Subs = append(
		block.Subs,
		&Block{
			Label:   inLabel,
			Content: inContent,
		},
	)
}

// AddContentWithCodec appends a new block with the given content buf an accompanying multicodec path
func (block *Block) AddContentWithCodec(inContent []byte, inCodec string) {
	block.Subs = append(
		block.Subs,
		&Block{
			Codec:   inCodec,
			Content: inContent,
		},
	)
}
