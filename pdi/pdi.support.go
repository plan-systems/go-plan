// See pdi.proto

package pdi

import (

    "io"
    "strconv"


	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
	//"github.com/plan-systems/go-plan/pdi"

    //"golang.org/x/crypto/sha3"

    //"github.com/ethereum/go-ethereum/common/hexutil"

    "encoding/base64"
)



// StorageEpochFilename is the default file name used to store the latest StorageEpoch
const StorageEpochFilename = "StorageEpoch.json"

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


// SegmentIntoTxns is a utility that chops up a payload buffer into segments <= inMaxSegmentSize
func SegmentIntoTxns(
	inPayload         []byte,
    inPayloadEnc      plan.Encoding, 
	inMaxSegmentSize  uint32,
) (*PayloadTxnSet, error) {

    payloadSz := uint32(len(inPayload))
	bytesRemain := uint32(payloadSz)

	N := (payloadSz + inMaxSegmentSize - 1) / inMaxSegmentSize
    pos := uint32(0)

    txnSet := NewTxnSet(N)
    txnSet.NewlyAuthored = true
    txnSet.NumSegsMissing = 0

    for i := uint32(0); i < N; i++ {

        seg := NewDecodedTxn(nil)
        txnSet.Segs[i] = seg

		segSz := bytesRemain
		if segSz > inMaxSegmentSize {
			segSz = inMaxSegmentSize
		}

        seg.Info.PayloadEncoding = inPayloadEnc
        seg.Info.SegSz = segSz
		seg.Info.SegIndex = i
		seg.Info.SegTotal = N
        seg.PayloadSeg = inPayload[pos:pos+segSz]

		pos += segSz
        bytesRemain -= segSz
    }

    plan.Assert(bytesRemain == 0, "bytesRemain != 0")

	return txnSet, nil

}





// Base64p is a base64 char set that such that values are sortable when encoded (each glyph has an increasing ASCII value).Base64.
// See comments for TxnInfo.URID in pdi.proto
var Base64p = base64.NewEncoding(plan.Base64pCharSet).WithPadding(base64.NoPadding)


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

    // URIDSz is the total bytesize of a decoded URID.
    // URID aka "Universal Transaction Identifier", an ASCII string that encodes 33 bytes using pdi.Base64: 
    //     6 bytes (rightmost BIG-endian bytes of TimeSealed) 
    //  + 27 bytes (rightmost-bytes of hash digest of this txn) ==> *33* bytes (total) ==> *44* chars (pdi.Base64 encoded)
    URIDSz = URIDTxnIDSz + URIDTimestampSz

    // URIDTimestampStrLen is the base64 char len of an encoded timestamp.  To the right of this position, the txn hashname begins.
    URIDTimestampStrLen = 8 * URIDTimestampSz / 6

    // URIDStrLen is the ASCII char length of an encoded URID (44 chars)
    URIDStrLen = 8 * URIDSz / 6

)

// Encode64 encodes the given binary buffer into base64.
func Encode64(in []byte) string {
	return Base64p.EncodeToString(in)
}

// URID aka "Universal Resource Identifier"
// 
// The purpose of a URID is that it can be easily compared with others and easily sorted chronologically.
type URID []byte

// URIDBlob is an array buf version of URID
type URIDBlob [URIDSz]byte

// Blob is a convenience function that forms a URID byte array from a URID byte slice. 
func (id URID) Blob() URIDBlob {

    var blob URIDBlob
    copy(blob[:], id)

    return blob
}

// String converts a binary URID into its pdi.Base64 ASCII string representation.
func (id URID) String() string {
    var str [URIDStrLen]byte

    sz := len(id)
    if sz == URIDSz {
        sz = URIDStrLen
    } else if sz == URIDTimestampSz {
        sz = URIDTimestampStrLen
    } else {
        return ""
    }
        
    Base64p.Encode(str[:], id)
	return string(str[:sz])  
}


// ExtractTime extracts the time index bourne by this URID
func ExtractTime(inURID []byte) int64 {

    t := int64(inURID[0]) 
    t = (t << 8) | int64(inURID[1]) 
    t = (t << 8) | int64(inURID[2]) 
    t = (t << 8) | int64(inURID[3]) 
    t = (t << 8) | int64(inURID[4]) 
    t = (t << 8) | int64(inURID[5]) 

    return t
}


// URIDFromInfo returns the binary/base256 form of a binary URID aka "Universal Transaction Identifier"
//
// If in is set, the new URID written starting at pos 0 (assuming it has the capacity), otherwise a new buf is allocated.
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
        if cap(in) >= utidLen {
            raw = in[:utidLen]
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



// FormMemberStrID returns a base 10 string of the member ID associated with this MemeberEpoch
func (epoch *MemberEpoch) FormMemberStrID() string {
    return strconv.FormatUint(uint64(epoch.MemberID), 10)
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

// ChID returns the channel ID for this entry.
func (entry *EntryInfo) ChID() plan.ChID {
    return plan.ChID(entry.ChannelID)
}

// GetTID returns a slice to requested EntryTID
func (entry *EntryInfo) GetTID(inID EntryTID) plan.TID {
    pos := int(inID) * plan.TIDSz
    end := pos + plan.TIDSz
    if end > len(entry.TIDs) {
        return nil
    }

    return entry.TIDs[pos:end]
}

// EntryID returns the entry TID for this entry.
//
// Note: if this entry is in the process of being authored/formed, the hash portion of the TID is undefined since
// the hashname of the entry is not yet known.
func (entry *EntryInfo) EntryID() plan.TID {
    return entry.GetTID(EntryTID_EntryID)
}

// TimeAuthoredFS returns a unix timestamp (in 1<<16 seconds) of when is entry was authored.
func (entry *EntryInfo) TimeAuthoredFS() plan.TimeFS {
    entryTID := entry.GetTID(EntryTID_EntryID)
    return entryTID.ExtractTimeFS()
}

// TimeAuthored returns a unix timestamp (in seconds) of when is entry was authored.
func (entry *EntryInfo) TimeAuthored() int64 {
    entryTID := entry.GetTID(EntryTID_EntryID)
    return entryTID.ExtractTime()
}

// SetTimeAuthored sets the time in this entry's TID, initializing as necessary.
func (entry *EntryInfo) SetTimeAuthored(inTimeFS plan.TimeFS) {
    
    if inTimeFS <= 0 {
        inTimeFS = plan.NowFS()
    }

    N := int(EntryTID_NormalNumTIDs) * plan.TIDSz
    if cap(entry.TIDs) < N {
        entry.TIDs = make([]byte, N)
    } else {
        entry.TIDs = entry.TIDs[:N]
    }
    
    entry.EntryID().SetTimeFS(inTimeFS)
}

// IsChannelGenesis returns true if this channel epoch implies the creation/genesis of a new channel
func (epoch *ChannelEpoch) IsChannelGenesis() bool {
    return len(epoch.PrevEpochTID) == 0;
}
// HasACC returns true if this ChannelEpoch has a non-nil ACC set
func (epoch *ChannelEpoch) HasACC() bool {
    return ! plan.TID(epoch.ACC).IsNil()
}
