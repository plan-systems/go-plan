package bufs

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"reflect"
)

var (
	// Base64Encoding is used to encode/decode binary buffer to/from base 64
	Base64Encoding = base64.RawURLEncoding

	// GenesisMemberID is the genesis member ID
	GenesisMemberID = uint32(1)
)

// Zero zeros out a given slice
func Zero(buf []byte) {
	N := int32(len(buf))
	for i := int32(0); i < N; i++ {
		buf[i] = 0
	}
}

// Marshalable generalizes efficient serialization
type Marshalable interface {
	Marshal() ([]byte, error)
	MarshalToSizedBuffer([]byte) (int, error)
	Size() int
}

// Unmarshalable used to generalize deserialization
type Unmarshalable interface {
	Unmarshal([]byte) error
}

// SmartMarshal marshals the given item to the given buffer.  If there is not enough space a new one is allocated.  The purpose of this is to reuse a scrap buffer.
func SmartMarshal(item Marshalable, tryBuf []byte) []byte {
	var err error

	bufSz := cap(tryBuf)
	neededSz := item.Size()
	if neededSz > bufSz {
		neededSz = (neededSz + 7) &^ 7
		tryBuf = make([]byte, neededSz)
		bufSz = neededSz
	}

	neededSz, err = item.MarshalToSizedBuffer(tryBuf[:bufSz])
	if err != nil {
		panic(err)
	}

	return tryBuf[:neededSz]
}

// SmartMarshalToBase64 marshals the given item and then encodes it into a base64 (ASCII) byte string.
//
// If tryDst is not large enough, a new buffer is allocated and returned in its place.
func SmartMarshalToBase64(src Marshalable, tryDst []byte) []byte {
	bufSz := cap(tryDst)
	binSz := src.Size()
	neededSz := 4 + 4*((binSz+2)/3)
	if neededSz > bufSz {
		neededSz = (neededSz + 7) &^ 7
		tryDst = make([]byte, neededSz)
		bufSz = neededSz
	}

	// First, marshal the item to the right-side of the scrap buffer
	binBuf := tryDst[bufSz-binSz : bufSz]
	var err error
	binSz, err = src.MarshalToSizedBuffer(binBuf)
	if err != nil {
		panic(err)
	}

	// Now encode the marshaled to the left side of the scrap buffer.
	// There is overlap, but encoding consumes from left to right, so it's safe.
	encSz := Base64Encoding.EncodedLen(binSz)
	tryDst = tryDst[:encSz]
	Base64Encoding.Encode(tryDst, binBuf[:binSz])

	return tryDst
}

// SmartDecodeFromBase64 decodes the base64 (ASCII) string into the given scrap buffer, returning the scrap buffer set to proper size.
//
// If tryDst is not large enough, a new buffer is allocated and returned in its place.
func SmartDecodeFromBase64(srcBase64 []byte, tryDst []byte) ([]byte, error) {
	binSz := Base64Encoding.DecodedLen(len(srcBase64))

	bufSz := cap(tryDst)
	if binSz > bufSz {
		bufSz = (binSz + 7) &^ 7
		tryDst = make([]byte, bufSz)
	}
	var err error
	binSz, err = Base64Encoding.Decode(tryDst[:binSz], srcBase64)
	return tryDst[:binSz], err
}

// Buf is a flexible buffer designed for reuse.
type Buf struct {
	Unmarshalable

	Bytes []byte
}

// Unmarshal effectively copies the src buffer.
func (buf *Buf) Unmarshal(srcBuf []byte) error {
	N := len(srcBuf)
	if cap(buf.Bytes) < N {
		allocSz := ((N + 127) >> 7) << 7
		buf.Bytes = make([]byte, N, allocSz)
	} else {
		buf.Bytes = buf.Bytes[:N]
	}
	copy(buf.Bytes, srcBuf)

	return nil
}

var (
	bytesType = reflect.TypeOf(Bytes(nil))
)

// Bytes marshal/unmarshal as a JSON string with 0x prefix.
// The empty slice marshals as "0x".
type Bytes []byte

// MarshalText implements encoding.TextMarshaler
func (b Bytes) MarshalText() ([]byte, error) {
	out := make([]byte, len(b)*2+2)
	out[0] = '0'
	out[1] = 'x'
	hex.Encode(out[2:], b)
	return out, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(in []byte) error {
	if !isString(in) {
		return errNonString(bytesType)
	}
	return wrapTypeError(b.UnmarshalText(in[1:len(in)-1]), bytesType)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *Bytes) UnmarshalText(input []byte) error {
	raw, err := checkText(input)
	if err != nil {
		return err
	}
	dec := make([]byte, len(raw)/2)
	if _, err = hex.Decode(dec, raw); err == nil {
		*b = dec
	}
	return err
}

// String returns the hex encoding of b.
func (b Bytes) String() string {
	out := make([]byte, len(b)*2+2)
	out[0] = '0'
	out[1] = 'x'
	hex.Encode(out[2:], b)
	return string(out)
}

func isString(input []byte) bool {
	return len(input) >= 2 && input[0] == '"' && input[len(input)-1] == '"'
}

func wrapTypeError(err error, typ reflect.Type) error {
	if _, ok := err.(*encodingErr); ok {
		return &json.UnmarshalTypeError{Value: err.Error(), Type: typ}
	}
	return err
}

func errNonString(typ reflect.Type) error {
	return &json.UnmarshalTypeError{Value: "non-string", Type: typ}
}

func checkText(in []byte) ([]byte, error) {
	N := len(in)
	if N == 0 {
		return nil, nil // empty strings are allowed
	}
	if N >= 2 && in[0] == '0' && (in[1] == 'x' || in[1] == 'X') {
		in = in[2:]
		N -= 2
	}
	return in, nil
}
