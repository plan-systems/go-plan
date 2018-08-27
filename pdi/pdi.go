

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





/*
// Provider wraps how an SKI connection is implemented.  Perhaps it's locally implemented, or perhaps the it uses a network connection.
type ReplicatorService interface {

    // StartSession starts a new session SKI.session.  In general, you should only start one session 
    StartSession(
        inInvocation        string,
        inSyncFromTime      int64,
        inClient            ReplicatorClient,
    ) *plan.Perror

    // DispatchOp implements a complete set of SKI ops
    PublishEntry(inClient *EtryCrypt, inOnProgress ProgressHandler)


}

type ReplicatorClient interface {

    OnSessionStarted()

    OnSessionEnded()

    OnEntryArrived()
}



type ProgresReport struct {
    StepNum     int     // Crurent step number (0=setting up, TotalSteps=complete; last call!
    StepDesc    string  // Description of the currenrt step
    TotalSteps  int     // Total number of steps in this job (or 0 if unknown)
}

  
  // ProgressHandler handles the result of a SKI operation
type ProgressHandler func(inErr *plan.Perror, inReport ProgressReport, inBody *pdi.Body)



// Session provides lambda-lifted crypto services from an opaque service provider. 
// All calls in this interface are threadsafe.
type ReplicatorSession interface {

    // DispatchOp implements a complete set of SKI ops
    PublishEntry(inEntry *EntryCrypt, inOnProgress ProgressHandler)

    // EndSession ends this SKI session, resulting in the SKI's parent Provider to call its OnSessionClosed() callback followed by inOnCompletion.
    // Following a call to EndSession(), no more references to this session should be made -- Provider.StartSession() must be called again.
    EndSession(inReason string, inOnCompletion plan.Action)

}




message ReplicatorTxn {



    // ---  TXN METADATA  ---

    // Issued by the a Replicator -- identifies this block externally
                bytes           hashname                = 1;

    // Reconstructed time index that may improve over time based on consensus convergence -- 0 if not known.
                int64           time_consensus          = 2;

    // When a txn is "final", there is a 0 probabilty that it will be revoked,
                bool            is_final                = 3;

    // Unix timestamp of when this txn was first published to the replicator
                int64           time_published          = 2;


    // ---  TXN PAYLOAD  ---

    // Allows 
                int32           part_num                = 3;    
                int32           num_parts               = 3;
                bytes           part_data               = 6;
}





service Replicator {

    rpc         StartSession(SessionRequest)                    returns (SessionInfo);

    //rpc         ResetReadHead()
}


*/