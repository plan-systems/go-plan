// See pdi.proto

package pdi

import (
	"encoding/binary"

	"github.com/plan-tools/go-plan/plan"

	"github.com/ethereum/go-ethereum/crypto/sha3"
)

// EntryVersionMask is a bit mask on EntryCrypt.CryptInfo to extract pdi.EntryVersion
const EntryVersionMask = 0xFF

// GetEntryVersion returns the version of this entry (should match EntryVersion1)
func (entry *EntryCrypt) GetEntryVersion() EntryVersion {
	return EntryVersion(entry.CryptInfo & EntryVersionMask)
}

// ComputeHash hashes all fields of psi.EntryCrypt (except .EntrySig)
func (entry *EntryCrypt) ComputeHash() []byte {

	hw := sha3.NewKeccak256()

	var scrap [16]byte

	pos := 0
	pos = encodeVarintPdi(scrap[:], pos, entry.CryptInfo)

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityKeyId)
	hw.Write(entry.HeaderCrypt)
	hw.Write(entry.BodyCrypt)

	return hw.Sum(nil)

}

// StorageTxnNameSz is the size of a StorageTxn hashname (2^256)
const StorageTxnNameSz = 32

// TimeSortableKeySz reflects the size of a uint64 plus the size of a StorageTxnName
const TimeSortableKeySz = 8 + StorageTxnNameSz

// TimeSortableKey is concatenation of a 8-byte unix timestamp followed by a StorageTxnName (hashname).
// This allows efficient time-based sorting of StorageTxn hashnames.  For a hash collision to occur with these sizes,
//     the txns would have to occur during the *same* second AND be 1 in 10^77 (AND would have to occur in the *same* communty)
type TimeSortableKey [TimeSortableKeySz]byte

// Increment "adds 1" to a TimeSortableKey, alloing a search routine to increment to the next possible hashname.
// Purpose is to increment.  E.g.
//   ... 39 00 => ... 39 01
//   ... 39 01 => ... 39 02
//            ...
//   ... 39 ff => ... 3A 00
func (tk *TimeSortableKey) Increment() {


	// pos says which byte-significan't digit we're on -- start at the least signigicant
	pos := TimeSortableKeySz - 1
	for {

		// Increment and stop if there's no carry
		tk[pos]++
		if tk[pos] > 0 {
			break
		}

		// We're here because there's a carry -- so move to the next byte digit
		pos--
	}
}

// GetTimeSortableKey lays out a unix timestamp in 8 bytes in big-endian followed by and a TimeSortableKey (a hashname).
// This gives these time+hashname keys (and their accompanying value) the property to be stored, sorted by timestamp. 
func GetTimeSortableKey(inTime int64, inTxnName []byte) TimeSortableKey {
	var k TimeSortableKey

	binary.BigEndian.PutUint64(k[0:8], uint64(inTime))

	overhang := StorageTxnNameSz - len(inTxnName)
	if overhang < 0 {
		copy(k[8:], inTxnName[-overhang:])
	} else {
		copy(k[8+overhang:], inTxnName)
	}

	return k
}

/*****************************************************
** Utils
**/

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

/*
// SegmentIntoTxnsForMaxSize is a utility that chops up a payload buffer into segments <= inMaxSegmentSize
func SegmentIntoTxnsForMaxSize(
	inData []byte,
	inDataDesc TxnDataDesc,
	inMaxSegmentSize int,
) ([]*StorageTxn, error) {

	bytesRemain := len(inData)
	pos := 0

	N := (len(inData) + inMaxSegmentSize - 1) / inMaxSegmentSize
	txns := make([]*StorageTxn, 0, N)

	for bytesRemain > 0 {

		segSz := bytesRemain
		if segSz < inMaxSegmentSize {
			segSz = inMaxSegmentSize
		}

		txns = append(txns, &StorageTxn{
			TxnStatus:   TxnStatus_AWAITING_COMMIT,
			DataDesc:    inDataDesc,
			SegmentData: inData[pos:segSz],
		})

		pos += segSz
	}

	for i, txn := range txns {
		txn.SegmentNum = uint32(i)
		txn.TotalSegments = uint32(len(txns))
	}

	return txns, nil

	//if bytesRemain != 0 {
	//    return plan.Error(nil, plan.AssertFailure, "assertion failed in SegmentPayloadForSegmentSize {N:%d, bytesRemain:%d}", N, bytesRemain)
	//}
}
*/
