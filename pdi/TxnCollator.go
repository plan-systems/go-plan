package pdi

import (
    "bytes"
    "sync"

	"github.com/plan-systems/go-plan/plan"
)


// PayloadHandler is the callback made when a txn segment series is complete and reassembled.
type PayloadHandler func(inTxnSet *PayloadTxnSet) error

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
    PayloadID       plan.TIDBlob

    scrap           bytes.Buffer
}



var txnPool = sync.Pool{
	New: func() interface{} {
		return new(DecodedTxn)
	},
}

// NewDecodedTxn allocates/recycles a DecodedTxn as if it was newly instantiated.
func NewDecodedTxn() *DecodedTxn {
    seg := txnPool.Get().(*DecodedTxn)

    seg.RawTxn = seg.RawTxn[:0]
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
    txnSet.PayloadID = plan.NilTID

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

// AccessPayload re-assumbles the payload 
func (txnSet *PayloadTxnSet) AccessPayload(
    inReader func(inPayload []byte) error,
) error {

    var err error

    info := txnSet.Info()
    if info == nil {
        err = plan.Error(nil, plan.ParamMissing, "no txns to unmarshal")
    } else if txnSet.NumSegsMissing > 0 {
        err = plan.Error(nil, plan.ParamMissing, "payload txn set not complete")
    } else {

        if info.SegTotal == 1 {

            // If only a single txn, nothing to reassemble
            err = inReader(txnSet.Segs[0].PayloadSeg)

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
                err = inReader(txnSet.scrap.Next(totalSz))
            }
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

// MergeSegment adds the given txn to this segment group
func (txnSet *PayloadTxnSet) MergeSegment(seg *DecodedTxn) bool {

	idx := seg.Info.SegIndex

	if txnSet.Segs[idx] == nil {
        if txnSet.NumSegsMissing > 0 {
		    txnSet.NumSegsMissing--
        }
	}
	txnSet.Segs[idx] = seg
	
    return txnSet.NumSegsMissing == 0
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

// URIDstring returns the string-ified URID associated with this txn
func (seg *DecodedTxn) URIDstring() string {
    return URID(seg.Info.URID).String()
}

// TxnCollater helps desegment/"collate" txns from multi-segment pieces into a single segment (that can be unmarshalled).
type TxnCollater struct {
    PayloadHandler PayloadHandler
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
) error {

    seg := NewDecodedTxn()
    seg.RawTxn = inRawTxn.Bytes

    err := seg.DecodeRawTxn(inDecoder)
    if err != nil {
        return err
    }

    if len(inRawTxn.URID) > 0 && ! bytes.Equal(seg.Info.URID, inRawTxn.URID) {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "txn URID did not match given label") 
    }

    return tc.Desegment(seg)
}
 

// Consolidate collates multisegment txns until all the segments are present.
//
// If txnIn completes this multi-segment txn group, the txnIncoming returned is a reconstructed (single) segment ready for decapsulation.
func (tc *TxnCollater) consolidate(txnSet *PayloadTxnSet) error {

    info := txnSet.Info()
    if info == nil {
        return plan.Error(nil, plan.TxnNotConsistent, "no txn segments to consolidate")
    }

	// Exit if there's still more segments to go
	if txnSet.NumSegsMissing > 0 {
		return plan.Errorf(nil, plan.TxnNotConsistent, "missing %d out of %d segments", txnSet.NumSegsMissing, info.SegTotal)
	}

	var prevURID []byte

	// First verify all segments present and calc size
	for _, seg := range txnSet.Segs {

        if seg.Info.SegTotal != info.SegTotal || seg.Info.PayloadEncoding != info.PayloadEncoding {
            return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v failed group consistency check", seg.URIDstring())
        } else if ! bytes.Equal(seg.Info.PrevURID, prevURID) {
			return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expects prev seg URID %v, got %v", seg.URIDstring(), seg.Info.PrevURID, prevURID)
		}

		prevURID = seg.Info.URID
	}

    return tc.PayloadHandler(txnSet)
}


// Desegment collects decoded txns (payload segments) and returns ones that are a single segment.
func (tc *TxnCollater) Desegment(
    seg *DecodedTxn,
) error {

	var (
		err  error
	)
	segSz := uint32(len(seg.PayloadSeg))

    segURID := seg.TxnURID()

	// If there's only a single segment, we can decode immediately.
	if seg.Info.SegTotal < 1 || seg.Info.SegIndex >= seg.Info.SegTotal {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "bad txn %v, SegIndex=%d SegTotal=%d", seg.URIDstring(), seg.Info.SegIndex, seg.Info.SegTotal)
    } else if seg.Info.SegSz != segSz {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v bad seg len: expected %d, got %d", seg.URIDstring(), seg.Info.SegSz, segSz)
    } else if seg.Info.SegIndex == 0 && len(seg.Info.PrevURID) != 0 {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v has illegal PrevURID", seg.URIDstring())
	} else if seg.Info.SegTotal == 1 {
        txnSet := NewTxnSet(1)
        txnSet.MergeSegment(seg)
        err = tc.PayloadHandler(txnSet)
	} else {
        
        txnURID := segURID.Blob()
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
        
                canConsolidate := txnSet.MergeSegment(entry.seg)
                if canConsolidate {

                    // Detach each txn segment
                    for _, seg := range txnSet.Segs {
                        delete(tc.segMap, seg.TxnURID().Blob())
                    }

                    err = tc.consolidate(txnSet)
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

	return err
}
