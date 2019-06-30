package pdi

import (
    "bytes"
    "sync"

	"github.com/plan-systems/plan-core/tools"
	"github.com/plan-systems/plan-core/plan"
)

// DecodedTxn contains the contents of a decoded raw native txn from a StorageProvider
type DecodedTxn struct {
    RawTxn     []byte
	Info       TxnInfo
	PayloadSeg []byte       // Points to another slice; read only
}

// PayloadTxnSet represents aset of txns desegmented into the initial payload.
type PayloadTxnSet struct {
    Segs            []*DecodedTxn
    NumSegsMissing  uint32
    NewlyAuthored   bool
    PayloadTID      plan.TIDBlob

    scrap           bytes.Buffer
}

var txnPool = sync.Pool{
	New: func() interface{} {
		return new(DecodedTxn)
	},
}

// NewDecodedTxn allocates/recycles a DecodedTxn as if it was newly instantiated.
func NewDecodedTxn(inRawTxn []byte) *DecodedTxn {
    seg := txnPool.Get().(*DecodedTxn)

    txnLen := len(inRawTxn)

    if txnLen == 0 {
        seg.RawTxn = seg.RawTxn[:0]
    } else if txnLen < 20000 && txnLen > cap(seg.RawTxn) {
        seg.RawTxn = append(seg.RawTxn[:0], inRawTxn...)
    } else {
        seg.RawTxn = inRawTxn
    }
    seg.PayloadSeg = nil
    seg.Info.Reset()

    return seg
}

// RecycleDecodedTxn turns the given instance back over to be reused (via NewDecodedTxn())
func RecycleDecodedTxn(inTxn *DecodedTxn) {
    txnPool.Put(inTxn)
}


var txnSetPool = sync.Pool{
	New: func() interface{} {
		return new(PayloadTxnSet)
	},
}

// NewTxnSet allocates/recycles a PayloadTxnSet, setting its state to as if it was freshly instantiated
// and then setting up the the number of requested segments.
func NewTxnSet(inNumSegs uint32) *PayloadTxnSet {
    txnSet := txnSetPool.Get().(*PayloadTxnSet)
    
    txnSet.NumSegsMissing = inNumSegs
    txnSet.NewlyAuthored = false
    txnSet.PayloadTID = plan.NilTID

    if cap(txnSet.Segs) < int(inNumSegs) {
        txnSet.Segs = make([]*DecodedTxn, inNumSegs)
    } else {
        txnSet.Segs = txnSet.Segs[:inNumSegs]

        for i := uint32(0); i < inNumSegs; i++ {
            if seg := txnSet.Segs[i]; seg != nil {
                RecycleDecodedTxn(seg)
                txnSet.Segs[i] = nil
            }
        }
    }

    return txnSet
}

// RecycleTxnSet adds the given item to be recycled/released.
//
// WARNING: after this call, there should be no dangling references to fields or values within the given item.
func RecycleTxnSet(inTxnSet *PayloadTxnSet) {
    N := len(inTxnSet.Segs)
    for i := 0; i < N; i++ {
        if seg := inTxnSet.Segs[i]; seg != nil {
            RecycleDecodedTxn(seg)
            inTxnSet.Segs[i] = nil
        }
    }
    txnSetPool.Put(inTxnSet)
}

// UnmarshalPayload re-assumbles the payload as necessary and unmarshals to the givne item
func (txnSet *PayloadTxnSet) UnmarshalPayload(
    outItem tools.Unmarshaller,
) error {

    info := txnSet.Info()

    var err error

    if info.SegTotal == 1 {

        // If only a single txn, nothing to reassemble
        err = outItem.Unmarshal(txnSet.Segs[0].PayloadSeg)

    } else {

        totalSz := 0
        for _, seg := range txnSet.Segs {
            totalSz += int(seg.Info.SegSz)
        }

        txnSet.scrap.Reset()
        txnSet.scrap.Grow(totalSz)

        for _, seg := range txnSet.Segs {
            txnSet.scrap.Write(seg.PayloadSeg)
        }

        if txnSet.scrap.Len() != totalSz {
            err = plan.Error(nil, plan.AssertFailed, "UnmarshalPayload assert failed")
        } else {
            err = outItem.Unmarshal(txnSet.scrap.Next(totalSz))
        }
    }

    return err
}

// Info returns TxnInfo about the underlying payload.
func (txnSet *PayloadTxnSet) Info() *TxnInfo {
    N := len(txnSet.Segs)
    for i := N-1; i >= 0; i-- {
        if seg := txnSet.Segs[i]; seg != nil {
            return &seg.Info
        }
    }
    return nil
}

// PayloadEncoding returns the encoding int associated with the payload contained in this txn set.
func (txnSet *PayloadTxnSet) PayloadEncoding() plan.Encoding {
    if info := txnSet.Info(); info != nil {
        return info.PayloadEncoding
    }
    return plan.Encoding_Unspecified
}

// Verify verifies that all the segments, verify all segments are linked\ and consistent
func (txnSet *PayloadTxnSet) Verify() error {

    info := txnSet.Info()

    if info == nil {
        return plan.Error(nil, plan.ParamErr, "no txns to present")
    } else if txnSet.NumSegsMissing > 0 {
        return plan.Errorf(nil, plan.ParamErr, "missing %d out of %d segments", txnSet.NumSegsMissing, info.SegTotal)
    } else {

        var prevURID []byte

        for _, seg := range txnSet.Segs {

            if seg == nil {
                return plan.Error(nil, plan.TxnNotConsistent, "missing at least one txn")
            } else if seg.Info.SegTotal != info.SegTotal || seg.Info.PayloadEncoding != info.PayloadEncoding {
                return plan.Errorf(nil, plan.TxnNotConsistent, "txn set %v not consistent", seg.URIDStr())
            } else if ! bytes.Equal(seg.Info.PrevURID, prevURID) {
                return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expects prev seg URID %v, got %v", seg.URIDStr(), seg.Info.PrevURID, prevURID)
            }

            prevURID = seg.Info.URID
        }
    }

    return nil

}


// MergeSegment adds the given txn to this segment group
func (txnSet *PayloadTxnSet) MergeSegment(seg *DecodedTxn) error {

	idx := seg.Info.SegIndex
	segSz := uint32(len(seg.PayloadSeg))
 
    // Check segment/txn info is well-formed
	if idx < 0 || seg.Info.SegTotal < 1 || idx >= seg.Info.SegTotal {
		return plan.Errorf(nil, plan.TxnNotConsistent, "bad txn %v, SegIndex=%d SegTotal=%d", seg.URIDStr(), seg.Info.SegIndex, seg.Info.SegTotal)
    } else if seg.Info.SegSz != segSz {
		return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v bad seg len: expected %d, got %d", seg.URIDStr(), seg.Info.SegSz, segSz)
    } else if uint32(len(txnSet.Segs)) != seg.Info.SegTotal {
		return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: total segments (%d) is not consistent with txnSet", seg.URIDStr(), seg.Info.SegTotal)
    }

    // Put the segment in it's place.
    if txnSet.Segs[idx] == nil {
        if txnSet.NumSegsMissing > 0 {
            txnSet.NumSegsMissing--
        }
    }
    txnSet.Segs[idx] = seg

    // If seem to have all the segments, verify all segments are linked and consistent
    if txnSet.NumSegsMissing == 0 {
        return txnSet.Verify()
    } 

    return nil
}

// DecodeAndMergeTxn decodes the given txn and merges it with this PayloadTxnSet. Txns
// passed to this function are assumed to be part of the same txn set.  If not, an error
func (txnSet *PayloadTxnSet) DecodeAndMergeTxn(
    inDecoder TxnDecoder,
    inRawTxn []byte,
) error {

    txn := NewDecodedTxn(inRawTxn)
    err := txn.DecodeRawTxn(inDecoder)
    if err != nil {
        return err
    }
    
    return txnSet.MergeSegment(txn)
}
   
// DecodeRawTxn is a convenience function for TxnDecoder.DecodeRawTxn()
func (seg *DecodedTxn) DecodeRawTxn(
	inDecoder TxnDecoder,
) error {

    // Don't even try to decode the txn if it's suspiciously large
    if len(seg.RawTxn) > TxnSegmentMaxSz + 10000 {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "txn exceeds safe max segment limit")
    }

	var err error
	seg.PayloadSeg, err = inDecoder.DecodeRawTxn(seg.RawTxn, &seg.Info)
	if err != nil {
		return err
	}

    if len(seg.Info.URID) < URIDSz {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "invalid txn URID") 
    }
    
	return nil
}

// TxnURID returns the URID associated with this txn
func (seg *DecodedTxn) TxnURID() URID {
    return URID(seg.Info.URID)
}

// URIDStr returns the string-ified URID associated with this txn
func (seg *DecodedTxn) URIDStr() string {
    return URID(seg.Info.URID).Str()
}

// TxnCollater helps desegment/"collate" txns from multi-segment pieces into a single segment (that can be unmarshalled).
type TxnCollater struct {
    segMap         map[URIDBlob]segEntry
}

type segEntry struct {
    seg      *DecodedTxn           // if set, this is a seg waiting to be claimed 
    txnSet   *PayloadTxnSet     // if set, this is the segGroup waiting for this seg
}

// NewTxnCollater creates a new TxnCollater
func NewTxnCollater() TxnCollater {
	return TxnCollater{
		segMap: make(map[URIDBlob]segEntry),
	}
}

// DecodeAndCollateTxn decodes the given txn and attempts to "collate" it, meaning 
//    any payloads that are completed get a callback via PayloadHandler()
func (tc *TxnCollater) DecodeAndCollateTxn(
    inDecoder TxnDecoder,
    inRawTxn *RawTxn,
) (*PayloadTxnSet, error) {

    seg := NewDecodedTxn(inRawTxn.Bytes)

    err := seg.DecodeRawTxn(inDecoder)
    if err != nil {
        return nil, err
    }

    if len(inRawTxn.URID) > 0 && ! bytes.Equal(seg.Info.URID, inRawTxn.URID) {
        return nil, plan.Errorf(nil, plan.TxnFailedToDecode, "txn URID did not match given label") 
    }

    return tc.Desegment(seg)
}
 

// Desegment collects decoded txns (payload segments) and returns ones that are a single segment.
func (tc *TxnCollater) Desegment(
    seg *DecodedTxn,
) (outTxnSet *PayloadTxnSet, err error) {

    if seg.Info.SegTotal <= 1 {
        outTxnSet = NewTxnSet(1)
        err = outTxnSet.MergeSegment(seg)
	} else {

        txnURID := seg.TxnURID().Blob()
        entry := tc.segMap[txnURID]

        // Use the last segment as a catalyst to start a new txnSet
        if entry.txnSet == nil && seg.Info.SegIndex + 1 == seg.Info.SegTotal { 
            entry.txnSet = NewTxnSet(seg.Info.SegTotal) 
        }

        entry.seg = seg
        tc.segMap[txnURID] = entry

        // if entry.txnSet is set, that means the given txnSet is waiting on this segment
        if entry.txnSet != nil {
            txnSet := entry.txnSet

            // Advance the leading "edge" of the segGroup towards index 0 (the first segment in the group)
            for entry.seg != nil && err == nil {
        
                err = txnSet.MergeSegment(entry.seg)
                if txnSet.NumSegsMissing == 0 {

                    // Detach each txn segment
                    for _, seg := range txnSet.Segs {
                        delete(tc.segMap, seg.TxnURID().Blob())
                    }

                    outTxnSet = txnSet
                    break
                }

                if err != nil || entry.seg.Info.SegIndex == 0 {
                    break
                }

                prevURID := URID(entry.seg.Info.PrevURID).Blob()
                prev := tc.segMap[prevURID]
                prev.txnSet = txnSet
                tc.segMap[prevURID] = prev

                entry = prev
            }
        }
	}

	return outTxnSet, err
}
