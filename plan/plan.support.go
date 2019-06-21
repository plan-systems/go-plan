package plan

import (
	"bytes"
)

// BinEncode encodes the given binary buffer into PLAN's std encoding (BinEncoding)
func BinEncode(in []byte) string {
	return BinEncoding.EncodeToString(in)
}

// Blob is a convenience function that forms a ChID byte array from a ChID byte slice.
func (chID ChID) Blob() ChIDBlob {

	var blob ChIDBlob
	copy(blob[:], chID)

	return blob
}

// Str returns this channel ID in plan.BinEncoding form.
func (chID ChID) Str() string {
	return BinEncoding.EncodeToString(chID)
}

// Clone returns a duplicate of this ChID
func (chID ChID) Clone() ChID {
	dupe := make([]byte, ChIDSz)
	copy(dupe, chID)
	return dupe
}

// SuffixStr returns the last few digits of this ChID in string form (for easy reading, logs, etc)
func (chID ChID) SuffixStr() string {
	return BinEncoding.EncodeToString(chID[ChIDSz-6:])
}

// TID is a convenience function that returns the TID contained within this TIDBlob.
func (tid *TIDBlob) TID() TID {
	return tid[:]
}

// IsNil returns true if this TID length is 0 or is equal to NilTID
func (tid TID) IsNil() bool {
	if len(tid) != TIDSz {
		return false
	}

	if bytes.Equal(tid, NilTID[:]) {
		return true
	}

	return false
}

// Clone returns a duplicate of this TID
func (tid TID) Clone() TID {
	dupe := make([]byte, TIDSz)
	copy(dupe, tid)
	return dupe
}

// Blob is a convenience function that forms a TID byte array from a TID byte slice.
func (tid TID) Blob() TIDBlob {

	var blob TIDBlob
	copy(blob[:], tid)

	return blob
}

// Str returns this TID in plan.Base64 form.
func (tid TID) Str() string {
	return BinEncoding.EncodeToString(tid)
}

// SuffixStr returns the last few digits of this TID in string form (for easy reading, logs, etc)
func (tid TID) SuffixStr() string {
	return BinEncoding.EncodeToString(tid[TIDSz-6:])
}

// SetTimeAndHash writes the given timestamp and the right-most part of inSig into this TID.
//
// Byte layout is designed so that TIDs can be sorted by its leading embedded timestamp:
//    0:6   - Standard UTC timestamp in unix seconds (BE)
//    6:8   - Timestamp fraction (BE)
//    8:27  - Hash bytes
func (tid TID) SetTimeAndHash(inTime TimeFS, inHash []byte) {

	tid.SetTimeFS(inTime)
	tid.SetHash(inHash)
}

// SetHash set the sig/hash portion of this ID
func (tid TID) SetHash(inHash []byte) {

	const TIDHashSz = TIDSz - 8
	pos := len(inHash) - TIDHashSz
	if pos >= 0 {
		copy(tid[8:], inHash[pos:])
	} else {
		for i := 8; i < TIDSz; i++ {
			tid[i] = 0
		}
	}
}

// SetTimeFS writes the given timestamp into this TIS
func (tid TID) SetTimeFS(inTime TimeFS) {

	tid[0] = byte(inTime >> 56)
	tid[1] = byte(inTime >> 48)
	tid[2] = byte(inTime >> 40)
	tid[3] = byte(inTime >> 32)
	tid[4] = byte(inTime >> 24)
	tid[5] = byte(inTime >> 16)
	tid[6] = byte(inTime >> 8)
	tid[7] = byte(inTime)

}

// ExtractTimeFS returns the unix timestamp embedded in this TID (a unix timestamp in 1<<16 seconds UTC)
func (tid TID) ExtractTimeFS() TimeFS {

	t := int64(tid[0])
	t = (t << 8) | int64(tid[1])
	t = (t << 8) | int64(tid[2])
	t = (t << 8) | int64(tid[3])
	t = (t << 8) | int64(tid[4])
	t = (t << 8) | int64(tid[5])
	t = (t << 8) | int64(tid[6])
	t = (t << 8) | int64(tid[7])

	return TimeFS(t)
}

// ExtractTime returns the unix timestamp embedded in this TID (a unix timestamp in seconds UTC)
func (tid TID) ExtractTime() int64 {

	t := int64(tid[0])
	t = (t << 8) | int64(tid[1])
	t = (t << 8) | int64(tid[2])
	t = (t << 8) | int64(tid[3])
	t = (t << 8) | int64(tid[4])
	t = (t << 8) | int64(tid[5])

	return t
}

// SelectEarlier looks in inTime and if it is later than the time embedded in ioURID, then this function has no effect and returns false.
// If inTime is earlier than the embedded time, then ioURID is initialized to inTime (and zeroed out) and returns true.
func (tid TID) SelectEarlier(inTime TimeFS) bool {

	t := tid.ExtractTimeFS()

	// Timestamp value of 0 is reserved and should only reflect an invalid/uninitialized TID.
	if inTime < 0 {
		inTime = 0
	}

	if inTime < t || inTime == 0 {
		tid[0] = byte(inTime >> 56)
		tid[1] = byte(inTime >> 48)
		tid[2] = byte(inTime >> 40)
		tid[3] = byte(inTime >> 32)
		tid[4] = byte(inTime >> 24)
		tid[5] = byte(inTime >> 16)
		tid[6] = byte(inTime >> 8)
		tid[7] = byte(inTime)

		for i := 8; i < len(tid); i++ {
			tid[i] = 0
		}
		return true
	}

	return false
}

// CopyNext copies the given TID and increments it by 1, typically useful for seeking the next entry after a given one.
func (tid TID) CopyNext(inTID TID) {
	copy(tid, inTID)
	for j := len(tid) - 1; j > 0; j-- {
		tid[j]++
		if tid[j] > 0 {
			break
		}
	}
}

// ExtractChID returns the ChID for what is presumed to be a channel genesis entry (whose ChID is dervied from the EntryID)
func (tid TID) ExtractChID() ChID {
    return ChID(tid[TIDSz - ChIDSz:])
}

/*****************************************************
** Utility & Conversion Helpers
**/

// GetCommunityID returns the CommunityID for the given buffer
func GetCommunityID(in []byte) CommunityID {
	var out CommunityID

	copy(out[:], in)
	return out
}

/*

// Multiplex interleaves the bits of A and B such that a composite uint64 is formed.
//
// Generally, A and B are "counting-like" numbers that are associated with a unique issuance
// number (and are generally human readable).
// This ensures that the composite uint64 is also small and can be looked at by a human easily
// and can also be easily stored compressed.
// In PLAN, channel epoch IDs are generated from a memberID and their own self-issued ID value,
//     ensuring that epoch IDs can never collide (without having to use 20+ byte channel and epoch IDs).
func Multiplex(A, B uint32) uint64 {

    ax := uint64(A)
    bx := uint64(B) << 1

    place := byte(0)
    x := uint64(0)

    for (ax | bx) > 0 {
        x |= ((ax & 1) + (bx & 2)) << place
        ax >>= 1
        bx >>= 1
	    place += 2
    }

    return x
}

// Unplex is the inverse of Multiplex().
func Unplex(x uint64) (uint32, uint32) {

    xa := x
    xb := x >> 1

    place := byte(0)
    A := uint32(0)
    B := uint32(0)
    for xa > 0 {
        A |= uint32(xa & 1) << place
        B |= uint32(xb & 1) << place
        xa >>= 2
        xb >>= 2
        place++
    }

    return A, B
}

*/
