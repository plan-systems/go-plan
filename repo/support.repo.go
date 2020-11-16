package repo

import (
	"bytes"
	"strings"

	"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/device"
)

// TIDSz is the byte size of a TID, a hash with a leading embedded big endian binary time index.
const TIDSz = int(Const_TIDSz)

// TIDEncodedLen is the ASCII-compatible string length of a (binary) TID encoded into its base32 form.
const TIDEncodedLen = int(Const_TIDEncodedLen)

// nilTID is a zeroed TID that denotes a void/nil/zero value of a TID
var nilTID = TIDBuf{}

// AssignFromURI parses the given POSIX-style uri string and sets all the fields of this ChStateURI
//
// Input form: "[/]DomainName/ChID"
func (uri *ChStateURI) AssignFromURI(uriStr string) error {
	uri.StateURI = ""

    // Ignore leading path sep
    sep := strings.IndexByte(uriStr, '/')
    if sep == 0 {
        uriStr = uriStr[1:]
        sep = strings.IndexByte(uriStr, '/')
    }

	if sep >= 0 {
		uri.DomainName, uri.ChID = uriStr[:sep], uriStr[sep+1:]

		sep = strings.IndexByte(uri.ChID, '/')
		if sep >= 0 {
			uri.ChID, uri.StateURI = uri.ChID[:sep], uri.ChID[sep+1:]
		}
	} else {
		uri.DomainName = uriStr[:]
		uri.ChID = ""
	}

	var err error

	n := 0
	if len(uri.ChID) == TIDEncodedLen {
		if cap(uri.ChID_TID) < TIDSz {
			uri.ChID_TID = make([]byte, TIDSz)
		}
		n, err = bufs.Base32Encoding.Decode(uri.ChID_TID[:TIDSz], []byte(uri.ChID))
	}
	if err != nil || n != TIDSz {
		uri.ChID_TID = uri.ChID_TID[:0]
	}

	return nil
}

// FormChURI is the inverse of AssignFromURI()
func (uri *ChStateURI) FormChURI() (string, error) {

    if len(uri.DomainName) == 0 {
        return "", ErrCode_InvalidURI.ErrWithMsg("missing domain name")
    }

    var b strings.Builder
    b.Grow(127)
    b.WriteString(uri.DomainName)
    b.WriteByte('/')

    switch {

    case len(uri.ChID) > 0: {
        b.WriteString(uri.ChID)
    }

    case len(uri.ChID_TID) == TIDEncodedLen: {
        var chID [TIDEncodedLen]byte
        bufs.Base32Encoding.Encode(chID[:], uri.ChID_TID)
        b.Write(chID[:])
    }

    case len(uri.ChID_TID) > 0: {
        uri.ChID = bufs.Base32Encoding.EncodeToString(uri.ChID_TID)
        b.WriteString(uri.ChID)
    }

    default:
        return "", ErrCode_InvalidURI.ErrWithMsg("missing channel ID")

    }

    return b.String(), nil
}


// ChKey is a keypath used in a repo db
type ChKey []byte


// TID is a convenience function that returns the TID contained within this TIDBuf.
func (tid *TIDBuf) TID() TID {
	return tid[:]
}

// Base32 returns this TID in Base32 form.
func (tid *TIDBuf) Base32() string {
	return bufs.Base32Encoding.EncodeToString(tid[:])
}

// IsNil returns true if this TID length is 0 or is equal to NilTID
func (tid TID) IsNil() bool {
	if len(tid) == 0 {
		return true
	}

	if bytes.Equal(tid, nilTID[:]) {
		return true
	}

	return false
}

// Clone returns a duplicate of this TID
func (tid TID) Clone() TID {
	dupe := make([]byte, Const_TIDSz)
	copy(dupe, tid)
	return dupe
}

// Buf is a convenience function that make a new TIDBuf from a TID byte slice.
func (tid TID) Buf() TIDBuf {
	var blob TIDBuf
	copy(blob[:], tid)
	return blob
}

// Base32 returns this TID in Base32 form.
func (tid TID) Base32() string {
	return bufs.Base32Encoding.EncodeToString(tid)
}

// SuffixStr returns the last few digits of this TID in string form (for easy reading, logs, etc)
func (tid TID) SuffixStr() string {
    const summaryStrLen = 5

    R := len(tid)
	L := R - summaryStrLen
	if L < 0 {
		L = 0
	}
	return bufs.Base32Encoding.EncodeToString(tid[L:R])
}

// SetTimeAndHash writes the given timestamp and the right-most part of inSig into this TID.
//
// See comments for Const_TIDSz
func (tid TID) SetTimeAndHash(time device.TimeFS, hash []byte) {
	tid.SetTimeFS(time)
	tid.SetHash(hash)
}

// SetHash sets the sig/hash portion of this ID
func (tid TID) SetHash(hash []byte) {
	const TIDHashSz = int(Const_TIDSz - Const_TIDTimestampSz)
	pos := len(hash) - TIDHashSz
	if pos >= 0 {
		copy(tid[TIDHashSz:], hash[pos:])
	} else {
		for i := 8; i < int(Const_TIDSz); i++ {
			tid[i] = hash[i]
		}
	}
}

// SetTimeFS writes the given timestamp into this TIS
func (tid TID) SetTimeFS(t device.TimeFS) {
	tid[0] = byte(t >> 56)
	tid[1] = byte(t >> 48)
	tid[2] = byte(t >> 40)
	tid[3] = byte(t >> 32)
	tid[4] = byte(t >> 24)
	tid[5] = byte(t >> 16)
	tid[6] = byte(t >> 8)
	tid[7] = byte(t)
}

// ExtractTimeFS returns the unix timestamp embedded in this TID (a unix timestamp in 1<<16 seconds UTC)
func (tid TID) ExtractTimeFS() device.TimeFS {
	t := int64(tid[0])
	t = (t << 8) | int64(tid[1])
	t = (t << 8) | int64(tid[2])
	t = (t << 8) | int64(tid[3])
	t = (t << 8) | int64(tid[4])
	t = (t << 8) | int64(tid[5])
	t = (t << 8) | int64(tid[6])
	t = (t << 8) | int64(tid[7])

	return device.TimeFS(t)
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

// SelectEarlier looks in inTime a chooses whichever is earlier.
//
// If t is later than the time embedded in this TID, then this function has no effect and returns false.
//
// If t is earlier, then this TID is initialized to t (and the rest zeroed out) and returns true.
func (tid TID) SelectEarlier(t device.TimeFS) bool {

	TIDt := tid.ExtractTimeFS()

	// Timestamp of 0 is reserved and should only reflect an invalid/uninitialized TID.
	if t < 0 {
		t = 0
	}

	if t < TIDt || t == 0 {
		tid.SetTimeFS(t)
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
