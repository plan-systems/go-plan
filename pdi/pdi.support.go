// See pdi.proto

package pdi

import (

    "io"

	"github.com/plan-systems/go-plan/plan"
	//"github.com/plan-systems/go-plan/pdi"

    "golang.org/x/crypto/sha3"

    "github.com/ethereum/go-ethereum/common/hexutil"

    "encoding/base64"
)



// AccountAlloc specifies an deposit values for a given public key (used during storage genesis).
type AccountAlloc struct {
    PubKey                  hexutil.Bytes           `json:"pub_key"`
    Gas                     int64                   `json:"gas"`  
    Fiat                    int64                   `json:"fiat"`  
}

// GenesisEpochFilename is the default file name used to store the latest CommunityEpoch
const GenesisEpochFilename = "genesis.json"

// StorageEpoch contains core params req'd for a community (and StorageProviders for that community) 
type StorageEpoch struct {
    CommunityName           string                  `json:"community_name"`
    CommunityID             hexutil.Bytes           `json:"community_id"`
    GenesisID               hexutil.Bytes           `json:"genesis_id"`
    Salt                    hexutil.Bytes           `json:"salt"`
    StartTime               int64                   `json:"start_time"` 
    FuelPerKb               int64                   `json:"fuel_per_kb"`         
    FuelPerTxn              int64                   `json:"fuel_per_txn"`       // Txn fuel cost := FuelPerTxn + FuelPerKb * (len(rawTxn) >> 10)
}

// EntryVersionMask is a bit mask on EntryCrypt.CryptInfo to extract pdi.EntryVersion
const EntryVersionMask = 0xFF

// GetEntryVersion returns the version of this entry (should match EntryVersion1)
func (entry *EntryCrypt) GetEntryVersion() EntryVersion {
	return EntryVersion(entry.CryptInfo & EntryVersionMask)
}

// ComputeHash hashes all fields of psi.EntryCrypt (except .EntrySig)
func (entry *EntryCrypt) ComputeHash() []byte {

	hw := sha3.NewLegacyKeccak256()

	var scrap [16]byte

	pos := 0
	pos = encodeVarintPdi(scrap[:], pos, entry.CryptInfo)

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityKeyId)
	hw.Write(entry.HeaderCrypt)
	hw.Write(entry.BodyCrypt)

	return hw.Sum(nil)

}

// MarshalToBlock marshals this EntryCrypt into a generic plan.Block
func (entry *EntryCrypt) MarshalToBlock() *plan.Block {

    block := &plan.Block{
        CodecCode: plan.CodecCodeForEntryCrypt,
    }

    var err error
    block.Content, err = entry.Marshal()
    if err != nil {
        panic(err)
    }

    return block
}

/*****************************************************
** Utils
**/

/*
// MarshalForOptionalBody marshals txn so that it can be deserializaed via UnmarshalWithOptionalBody().
func (txn *StorageTxn) MarshalForOptionalBody(dAtA []byte) ([]byte, error) {

	// Store the body in a different segment so can load it optionally
	body := txn.Body
	txn.Body = nil

	// Make a scrap buffer big enough to hold StorageTxn (w/o a body) and the body  -- TODO: use a buffer pool
	headerSz := txn.Size()
	bodySz := body.Size()

	szNeeded := headerSz + bodySz + 32
	szAvail := cap(dAtA)
	if szAvail < szNeeded {
		dAtA = make([]byte, szNeeded+32000)
	} else {
		dAtA = dAtA[:szAvail]
	}

	var err error

	// Marshal the header, prepend the header byte length
	headerSz, err = txn.MarshalTo(dAtA[2:])
	dAtA[0] = byte((headerSz >> 1) & 0xFF)
	dAtA[1] = byte((headerSz) & 0xFF)
	if err == nil {
		bodySz, err = body.MarshalTo(dAtA[2+headerSz:])
		finalSz := 2 + headerSz + bodySz
		if finalSz < len(dAtA) {
			dAtA = dAtA[:finalSz]
		} else {
			err = plan.Error(err, plan.FailedToMarshal, "StorageTxn.MarshalWithOptionalBody() assert failed")
		}
	}

	return dAtA, err
}

// UnmarshalWithOptionalBody allows the caller to not unmarshal the body, saving on allocation and cycles
func (txn *StorageTxn) UnmarshalWithOptionalBody(dAtA []byte, inUnmarshalBody bool) error {
	dataLen := len(dAtA)
	if dataLen < 8 {
		return plan.Error(nil, plan.FailedToUnmarshal, "StorageTxn.UnmarshalWithOptionalBody() failed")
	}

	var headerSz uint
	headerSz = uint(dAtA[0]<<1) | uint(dAtA[1])
	err := txn.Unmarshal(dAtA[2 : 2+headerSz])
	if err != nil {
		return err
	}
	if inUnmarshalBody {
		if txn.Body == nil {
			txn.Body = &plan.Block{}
		}
		err = txn.Body.Unmarshal(dAtA[2+headerSz:])
	}

	return err

}



// UnmarshalEntries unmarshals txn.Body (created via MarshalEntries) into the EntryCrypts contained within it 
func (txn *StorageTxn) UnmarshalEntries(ioBatch []*EntryCrypt) ([]*EntryCrypt, error) {

    var err error

    N := len(txn.Body.Subs)

    for i := -1; i < N && err != nil ; i++ {

        var block *plan.Block
        if i == -1 {
            block = txn.Body
        } else {
            block = txn.Body.Subs[i]
        }
        if block.CodecCode == plan.CodecCodeForEntryCrypt {
            entry := &EntryCrypt{}
            err = entry.Unmarshal(block.Content)
            if err != nil {
                break
            }
            ioBatch = append(ioBatch, entry)
        }
    }

    return ioBatch, err
}
*/

// MarshalEntries marshals the given batch of entries into a single plan.Block
func MarshalEntries(inBatch []*EntryCrypt) *plan.Block {
    N := len(inBatch)

    var head *plan.Block

    if N == 1 {
        head = inBatch[0].MarshalToBlock()
    } else if N > 1 {
        
        head := &plan.Block{
            Subs: make([]*plan.Block, N),
        }
        for i := range inBatch {
            head.Subs[i] = inBatch[i].MarshalToBlock()
        }
    }

    return head
}


// WriteVarInt appends the given integer in variable length format
func WriteVarInt(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}

// AppendVarBuf appends the given buffer's length in bytes and the buffer
func AppendVarBuf(dAtA []byte, offset int, inBuf []byte) (int, error) {
    bufLen := len(inBuf)
    origOffset := offset
    offset = WriteVarInt(dAtA, offset, uint64(bufLen))

    remain := len(dAtA) - offset
    if remain < bufLen {
        return origOffset, io.ErrUnexpectedEOF
    }
    copy(dAtA[offset:], inBuf)
    return offset + bufLen, nil
}




// ReadVarBuf reads a buffer written by AppendVarBuf() and returns the offset
func ReadVarBuf(dAtA []byte, offset int) (int, []byte, error) {
	l := len(dAtA)
    
    var bufLen uint64
    for shift := uint(0); ; shift += 7 {
        if shift >= 31 {
            return offset, nil, ErrIntOverflowPdi
        }
        if offset >= l {
            return offset, nil, io.ErrUnexpectedEOF
        }
        b := dAtA[offset]
        offset++
        bufLen |= (uint64(b) & 0x7F) << shift
        if b < 0x80 {
            break
        }
    }

    start := offset
    offset += int(bufLen)

   if bufLen < 0 {
        return offset, nil, ErrInvalidLengthPdi
    }

    if offset > l {
        return  offset, nil, io.ErrUnexpectedEOF
    }

    return offset, dAtA[start:offset], nil
}




/*****************************************************
** Support
**/

/*
var storageMsgPool = sync.Pool{
    New: func() interface{} {
        return new(StorageMsg)
    },
}

// RecycleStorageMsg effectively deallocates the item and makes it available for reuse
func RecycleStorageMsg(inMsg *StorageMsg) {
    for _, txn := range inMsg.Txns {
        txn.Body = nil  // TODO: recycle plan.Blocks too
    }
    storageMsgPool.Put(inMsg)
}

// NewStorageMsg allocates a new StorageMsg
func NewStorageMsg() *StorageMsg {

    msg := storageMsgPool.Get().(*StorageMsg)
    if msg == nil {
        msg = &StorageMsg{}
    } else {
        msg.Txns = msg.Txns[:0]
        msg.AlertCode = 0
        msg.AlertMsg = ""
    }

    return msg
}

// NewStorageAlert creates a new storage msg with the given alert params
func NewStorageAlert(
    inAlertCode AlertCode, 
    inAlertMsg string,
    ) *StorageMsg {

    msg := NewStorageMsg()
    msg.AlertCode = inAlertCode
    msg.AlertMsg = inAlertMsg

    return msg 

}


*/




// SegmentIntoTxns is a utility that chops up a payload buffer into segments <= inMaxSegmentSize
func SegmentIntoTxns(
	inData           []byte,
    inPayloadCodec   PayloadCodec, 
	inMaxSegmentSize int32,
) ([]*TxnInfo, error) {

    payloadSz := int32(len(inData))
	bytesRemain := int32(payloadSz)

	N := (payloadSz + inMaxSegmentSize - 1) / inMaxSegmentSize
	segs := make([]*TxnInfo, 0, N)
    pos := int32(0)

	for bytesRemain > 0 {

		segSz := bytesRemain
		if segSz > inMaxSegmentSize {
			segSz = inMaxSegmentSize
		}

		segs = append(segs, &TxnInfo{
            PayloadCodec: inPayloadCodec,
            SegSz: segSz,
		})

		pos += segSz
        bytesRemain -= segSz
	}

	for i, seg := range segs {
		seg.SegIndex = uint32(i)
		seg.SegTotal = uint32(N)
	}

    plan.Assert(bytesRemain == 0, "assertion failed in SegmentIntoTxns {N:%d, bytesRemain:%d}", N, bytesRemain)

	return segs, nil

}






// Base64 is a base64 char set that such that values are sortable when encoded (each glyph has an increasing ASCII value).Base64.
// See comments for TxnInfo.UTID in pdi.proto
var Base64 = base64.NewEncoding("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~").WithPadding(base64.NoPadding)


const (

    // UTIDTimestampSz is the bytesize of the timestamp stored in a UTID
    // This must be a multiple of 3 so that the encoded length doesn't have any bit overhang.
    // 48 bits of unix seconds maxes around 8,800,000 CE, so we'll call that good for now.  :D 
    UTIDTimestampSz = 6

    // UTIDTxnIDSz is the byte length of the ID bytes encoded within in a UTID.
    // This is also chosen to be a multiple of 3 such that the encoded txn hashname falls on base64 digit boundaries. 
    UTIDTxnIDSz = 27

    // UTIDTotalSz is the total bytesize of a decoded UTID.
    // UTID aka "Universal Transaction Identifier", an ASCII string that encodes 33 bytes using pdi.Base64: 
    //     6 bytes (rightmost BIG-endian bytes of TimeSealed) 
    //  + 27 bytes (rightmost-bytes of hash digest of this txn) ==> *33* bytes (total) ==> *44* chars (pdi.Base64 encoded)
    UTIDTotalSz = UTIDTxnIDSz + UTIDTimestampSz

    // UTIDEncodedTimestmapLen is the base64 char len of an encoded timestamp.  To the right of this position, the txn hashname begins.
    UTIDEncodedTimestmapLen = 8 * UTIDTimestampSz / 6

    // UTIDEncodedLen is the ASCII char length of an encoded UTID (44 chars)
    UTIDEncodedLen = 8 * UTIDTxnIDSz / 6

)


// FormUTID converts a txn timestamp and its binary identifier into a UTID aka "Universal Transaction Identifier",
//     an ASCII string that encodes these two items in pdi.Base64 (see UTIDDecodedLen above).
//
// The purpose of a UTID is so that txns can be stored and traversed chronologically in O(1) time.
//
// If len(inID) == 0, then only the encoded timstamp is returned (8 base64 chars).
// If a UTID is less than 44 chars, it is assumed to be left-padded with "zeros" (the '0' char)
func FormUTID(inPrefix string, inTimestamp int64, inID []byte) string {
    var raw [UTIDTotalSz]byte

	raw[0] = byte(inTimestamp >> 40)
	raw[1] = byte(inTimestamp >> 32)
	raw[2] = byte(inTimestamp >> 24)
	raw[3] = byte(inTimestamp >> 16)
	raw[4] = byte(inTimestamp >> 8)
	raw[5] = byte(inTimestamp)

    rawLen := UTIDTimestampSz

    // Use right-most bytes
    idSz := len(inID)
    if idSz > 0 {
        rawLen += UTIDTxnIDSz
    	overhang := idSz - UTIDTxnIDSz
        if overhang > 0 {
            copy(raw[8:], inID[overhang:])
        } else {
            copy(raw[8-overhang:], inID)
        }
    }

    prefixLen := len(inPrefix)

    var out [64]byte
    if prefixLen > 0 {
        if prefixLen > 64 - UTIDEncodedLen {
            prefixLen = 64 - UTIDEncodedLen
        }
        copy(out[:prefixLen], []byte(inPrefix))
    }

	Base64.Encode(out[prefixLen:], raw[:rawLen])
	return string(out[:prefixLen+UTIDEncodedLen])
}



// Deposit deposits the given transfer into this account
func (acct *StorageAccount) Deposit(xfer *Transfer) error {

    acct.FuelBalance += xfer.Fuel
    acct.ManaBalance += xfer.Mana

    return nil
}


// Withdraw subtracts the given transfer amount from this account
func (acct *StorageAccount) Withdraw(xfer *Transfer) error {

    if xfer.Fuel < 0 {
        return plan.Errorf(nil, plan.TransferFailed, "fuel transfer amount can't be negative")
    }
    if xfer.Mana < 0 {
        return plan.Errorf(nil, plan.TransferFailed, "mana transfer amount can't be negative")
    }

    if acct.FuelBalance < xfer.Fuel {
        return plan.Error(nil, plan.TransferFailed, "insufficient fuel for transfer")
    }
    acct.FuelBalance -= xfer.Fuel

    if acct.ManaBalance < xfer.Mana {
        return plan.Error(nil, plan.TransferFailed, "insufficient mana for transfer")
    }
    acct.ManaBalance -= xfer.Mana

    return nil
}