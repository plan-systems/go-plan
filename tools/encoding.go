package tools

import (
	//"bytes"
	"encoding/hex"
    "encoding/json"
	//"fmt"
    "reflect"
)

// Zero zeros out a given slice
func Zero(buf []byte) {
	N := int32(len(buf))
	for i := int32(0); i < N; i++ {
		buf[i] = 0
	}
}

// Marshaller used to generalize serialization
type Marshaller interface {
	Marshal() ([]byte, error)
	MarshalTo([]byte) (int, error)
	Size() int
}

// Unmarshaller used to generalize deserialization
type Unmarshaller interface {
	Unmarshal([]byte) error
}

// SmartMarshal marshals the given item to the given buffer.  If there is not enough space
func SmartMarshal(inItem Marshaller, ioBuf []byte) []byte {
	var err error

	bufSz := cap(ioBuf)
	sz := inItem.Size()
	if sz > bufSz {
		sz = (sz+7) &^ 7
		ioBuf = make([]byte, sz)
		bufSz = sz
	}

	sz, err = inItem.MarshalTo(ioBuf[:bufSz])
	if err != nil {
		panic(err)
	}

	return ioBuf[:sz]
}


// Buf is a flexible buffer designed for reuse.
type Buf struct {
	Unmarshaller

	Bytes []byte
}

// Unmarshal effectively copies the src buffer.
func (buf *Buf) Unmarshal(inSrc []byte) error {
	N := len(inSrc)
	if cap(buf.Bytes) < N {
		allocSz := ((N + 127) >> 7) << 7
		buf.Bytes = make([]byte, N, allocSz)
	} else {
		buf.Bytes = buf.Bytes[:N]
	}
	copy(buf.Bytes, inSrc)

	return nil
}



var (
	bytesT  = reflect.TypeOf(Bytes(nil))
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
	if ! isString(in) {
		return errNonString(bytesT)
	}
	return wrapTypeError(b.UnmarshalText(in[1:len(in)-1]), bytesT)
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
	if _, ok := err.(*toolsErr); ok {
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
	} else {
		return nil, ErrMissingPrefix
	}
	if (N & 1) != 0 {
		return nil, ErrOddLength
	}
	return in, nil
}

