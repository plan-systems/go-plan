// See pdi.proto

package pdi

import (

    "io"
    crand "crypto/rand"

	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
	//"github.com/plan-systems/go-plan/pdi"

    //"golang.org/x/crypto/sha3"

    //"github.com/ethereum/go-ethereum/common/hexutil"

    "encoding/base64"
)


// StorageEpochFilename is the default file name used to store the latest StorageEpoch
const StorageEpochFilename = "StorageEpoch.json"

// EntryVersionMask is a bit mask on EntryCrypt.CryptInfo to extract pdi.EntryVersion
//const EntryVersionMask = 0xFF

/*
// GetEntryVersion returns the version of this entry (should match EntryVersion1)
func (entry *EntryCrypt) GetEntryVersion() EntryVersion {
	return EntryVersion(entry.CryptInfo & EntryVersionMask)
}

// ComputeDigest hashes all fields of psi.EntryCrypt (except .EntrySig)
func (entry *EntryCrypt) ComputeDigest() []byte {

	hw := sha3.NewLegacyKeccak256()

	var scrap [16]byte

	pos := 0
	pos = encodeVarintPdi(scrap[:], pos, entry.CryptInfo)

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityPubKey)
	hw.Write(entry.InfoCrypt)
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
}*/

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
*/

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
	inPayload         []byte,
    inPayloadName     []byte,
    inPayloadEnc      plan.Encoding, 
	inMaxSegmentSize uint32,
) ([]*TxnInfo, error) {

    payloadSz := uint32(len(inPayload))
	bytesRemain := uint32(payloadSz)

	N := (payloadSz + inMaxSegmentSize - 1) / inMaxSegmentSize
	segs := make([]*TxnInfo, 0, N)
    pos := uint32(0)

	for bytesRemain > 0 {

		segSz := bytesRemain
		if segSz > inMaxSegmentSize {
			segSz = inMaxSegmentSize
		}

		segs = append(segs, &TxnInfo{
            PayloadEncoding: inPayloadEnc,
            PayloadName: inPayloadName,
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
// See comments for TxnInfo.URID in pdi.proto
var Base64 = base64.NewEncoding(plan.Base64CharSet).WithPadding(base64.NoPadding)


const (

    // URIDTimestampSz is the bytesize of the timestamp stored in a URID
    // This must be a multiple of 3 so that the encoded length doesn't have any bit overhang.
    URIDTimestampSz = 6

    // URIDTimestampMax is the max time value 
    // 48 bits of unix seconds maxes around 8,800,000 CE, so we'll call that good for now.  :D 
    URIDTimestampMax = (1 << (URIDTimestampSz * 8) ) - 1

    // URIDTxnIDSz is the byte length of the ID bytes encoded within in a URID.
    // This is also chosen to be a multiple of 3 such that the encoded txn hashname falls on base64 digit boundaries. 
    URIDTxnIDSz = 27

    // URIDBinarySz is the total bytesize of a decoded URID.
    // URID aka "Universal Transaction Identifier", an ASCII string that encodes 33 bytes using pdi.Base64: 
    //     6 bytes (rightmost BIG-endian bytes of TimeSealed) 
    //  + 27 bytes (rightmost-bytes of hash digest of this txn) ==> *33* bytes (total) ==> *44* chars (pdi.Base64 encoded)
    URIDBinarySz = URIDTxnIDSz + URIDTimestampSz

    // URIDTimestampStrLen is the base64 char len of an encoded timestamp.  To the right of this position, the txn hashname begins.
    URIDTimestampStrLen = 8 * URIDTimestampSz / 6

    // URIDStrLen is the ASCII char length of an encoded URID (44 chars)
    URIDStrLen = 8 * URIDBinarySz / 6

)

// Encode64 encodes the given binary buffer into base64.
func Encode64(in []byte) string {
	return Base64.EncodeToString(in)
}


// URID aka "Universal Resource Identifier"
// 
// The purpose of a URID is that it can be easily compared with others and easily sorted chronologically.
type URID []byte

// String converts a binary URID into its pdi.Base64 ASCII string representation.
func (utid URID) String() string {
    var str [URIDStrLen]byte

    sz := len(utid)
    if sz == URIDBinarySz {
        sz = URIDStrLen
    } else if sz == URIDTimestampSz {
        sz = URIDTimestampStrLen
    } else {
        return ""
    }
        
    Base64.Encode(str[:], utid)
	return string(str[:sz])  
}


// URIDFromInfo returns the binary/base256 form of a binary URID aka "Universal Transaction Identifier"
func URIDFromInfo(in []byte, inTimestamp int64, inID []byte) URID {

    idSz := len(inID)
    utidLen := URIDTimestampSz
    if idSz > 0 {
        utidLen += URIDTxnIDSz
    }

    if inTimestamp > URIDTimestampMax {
        inTimestamp = URIDTimestampMax
    }

    var raw []byte
    {
        sz := len(in)
        needed := sz + utidLen
        if cap(in) >= needed {
            raw = in[sz:needed]
        } else {
            raw = make([]byte, utidLen)
        }
    }

	raw[0] = byte(inTimestamp >> 40)
	raw[1] = byte(inTimestamp >> 32)
	raw[2] = byte(inTimestamp >> 24)
	raw[3] = byte(inTimestamp >> 16)
	raw[4] = byte(inTimestamp >> 8)
	raw[5] = byte(inTimestamp)

    // Use right-most bytes
    if idSz > 0 {
        overhang := idSz - URIDTxnIDSz

        if overhang > 0 {
            copy(raw[6:], inID[overhang:])
        } else {
            for i := 6; i < -overhang; i++ {
                raw[i] = 0
            }
            copy(raw[6-overhang:], inID)
        }
    }

    return raw[:utidLen]
}


// Deposit deposits the given transfer into this account
func (acct *StorageAccount) Deposit(xfer *Transfer) error {

    acct.KbBalance += xfer.Kb
    acct.OpBalance += xfer.Ops

    return nil
}


// Withdraw subtracts the given transfer amount from this account
func (acct *StorageAccount) Withdraw(xfer *Transfer) error {

    if xfer.Kb < 0 {
        return plan.Errorf(nil, plan.TransferFailed, "fuel transfer amount can't be negative")
    }
    if xfer.Ops < 0 {
        return plan.Errorf(nil, plan.TransferFailed, "mana transfer amount can't be negative")
    }

    if acct.KbBalance < xfer.Kb {
        return plan.Error(nil, plan.TransferFailed, "insufficient fuel for transfer")
    }
    acct.KbBalance -= xfer.Kb

    if acct.OpBalance < xfer.Ops {
        return plan.Error(nil, plan.TransferFailed, "insufficient mana for transfer")
    }
    acct.OpBalance -= xfer.Ops

    return nil
}





// CommunityKeyringName returns the name of the community keyring name
func (epoch *CommunityEpoch) CommunityKeyringName() []byte {
    return epoch.CommunityID
}


// CommunityKeyRef retruns the KeyRef that references this the community key on a SKI session.
func (epoch *CommunityEpoch) CommunityKeyRef() ski.KeyRef {
    return ski.KeyRef{
        KeyringName: epoch.CommunityKeyringName(),
        PubKey: epoch.KeyInfo.PubKey,
    }
}



// FormGenesisKeyringName returns the name of the keyring name that stores keys created during community genesis.
func (epoch *CommunityEpoch) FormGenesisKeyringName() []byte {

    krName := make([]byte, len(epoch.CommunityID))
    copy(krName, epoch.CommunityID)

    krName[0] = byte(krName[0] + 1)

    return krName

}



// RegenMemberKeys generates new keys for the given member.
func (epoch *MemberEpoch) RegenMemberKeys(
    skiSession ski.Session,
    inCommunityEpoch *CommunityEpoch,
) error {

    keyInfo, err := ski.GenerateNewKey(
        skiSession,
        epoch.FormSigningKeyringName(inCommunityEpoch.CommunityID),
        ski.KeyInfo{
            KeyType: ski.KeyType_SigningKey,
            CryptoKit: inCommunityEpoch.KeyInfo.CryptoKit,
        },
    )
    if err != nil { return err }
    epoch.PubSigningKey = keyInfo.PubKey

    keyInfo, err = ski.GenerateNewKey(
        skiSession,
        epoch.FormSendingKeyringName(inCommunityEpoch.CommunityID),
        ski.KeyInfo{
            KeyType: ski.KeyType_AsymmetricKey,
            CryptoKit: inCommunityEpoch.KeyInfo.CryptoKit,
        },
    )
    if err != nil { return err }
    epoch.PubEncryptKey = keyInfo.PubKey

    return nil
}






// FormMemberStrID returns a Base64 representation of this MemberID
func (epoch *MemberEpoch) FormMemberStrID() string {

    var raw [plan.MemberIDSz]byte
    
    memberID := epoch.MemberID

    for i := 0; i < plan.MemberIDSz; i++ {
        raw[i] = byte(memberID)
        memberID >>= 8
    }
    
    return Base64.EncodeToString(raw[:])
}





// GenerateNewMemberID generates a random member ID
func (epoch *MemberEpoch) GenerateNewMemberID() {

    memberID := uint64(0)
  
    var buf [plan.MemberIDSz]byte
    crand.Read(buf[:])
    for i := 0; i < 8; i++ {
        memberID = (memberID << 8) | uint64(buf[i])
    }

    epoch.MemberID = memberID
}





// FormSigningKeyringName forms the signing keyring name (ski.Keyring.KeyringName) for this member.
func (epoch *MemberEpoch) FormSigningKeyringName(
    inCommunityID []byte,
) []byte {

    clen := len(inCommunityID)
    if clen < plan.MemberIDSz {
        clen = plan.MemberIDSz
    }
    krName := make([]byte, clen)
    copy(krName, inCommunityID)

    memberID := epoch.MemberID

    for i := 0 ; i < plan.MemberIDSz; i++ {
        krName[i] ^= byte(memberID)
        memberID >>= 8
    }

    return krName
}


// FormSendingKeyringName forms the sending keyring name (ski.Keyring.KeyringName) for this member.
func (epoch *MemberEpoch) FormSendingKeyringName(
    inCommunityID []byte,
) []byte {

    krName := epoch.FormSigningKeyringName(inCommunityID)
    krName[0] = byte(krName[0] + 1)

    return krName
}