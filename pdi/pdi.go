

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

    scrap := make([]byte, 32)

    pos := 0
    pos = encodeVarintPdi(scrap, pos, entry.CryptInfo)
    pos = encodeVarintPdi(scrap, pos, uint64(entry.TimeCreated))

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityKeyId[:])
	hw.Write(entry.HeaderCrypt)
	hw.Write(entry.BodyCrypt)
    
    return hw.Sum( nil )

}








// GetPartWithName returns the first-appearing BodyPart with the matching part and codec name
func (body *Body) GetPartWithName(inCodec string, inPartName string) *BodyPart {
    for _, part := range body.Parts {
        if part.Name == inPartName && part.ContentCodec == inCodec {
            return part
        }
    }
    return nil
}

// GetPartContentWithName returns the content of the first-appearing BodyPart with the matching part and codec name
func (body *Body) GetPartContentWithName(inCodec string, inPartName string) []byte {
    for _, part := range body.Parts {
        if part.Name == inPartName && part.ContentCodec == inCodec {
            return part.Content
        }
    }
    return nil
}

// GetPartContent returns the content of the first-appearing BodyPart with the matching codec name
func (body *Body) GetPartContent(inCodec string) []byte {
    for _, part := range body.Parts {
        if part.ContentCodec == inCodec {
            return part.Content
        }
    }
    return nil
}

// AppendPart appends the give body part to this Body's list of parts
func (body *Body) AppendPart(inPart *BodyPart) {
    body.Parts = append(body.Parts, inPart)
}



