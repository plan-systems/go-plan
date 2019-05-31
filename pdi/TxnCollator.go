package pdi

import (
    "bytes"
    //"sync"

	"github.com/plan-systems/go-plan/plan"
)

// DecodedTxn contains the contents of a decoded raw native txn from a StorageProvider
type DecodedTxn struct {
    RawTxn     []byte
	Info       TxnInfo
	PayloadSeg []byte
}

// PayloadTxns represents aset of txns desegmented into the initial payload.
type PayloadTxns struct {
	PayloadEncoding plan.Encoding
    PayloadID       []byte
    Txns            []RawTxn
    NewlyAuthored   bool
}

// PayloadHandler is the callback made when a txn segment series is complete and reassembled.
type PayloadHandler func(inPayload []byte, inTxnSet *PayloadTxns) error

// PayloadIDStr returns the payload name/ID as a base64 string.
func (txns *PayloadTxns) PayloadIDStr() string {
    return Base64p.EncodeToString(txns.PayloadID)
}

// PayloadIDSuffixStr returns a string of the last few bytes of the PayloadID (for easier reading etc)
func (txns *PayloadTxns) PayloadIDSuffixStr() string {
    return Base64p.EncodeToString(txns.PayloadID[len(txns.PayloadID)-6:])
}

// AssignFromSegments exports the given txns into a finalized single virtual segments
func (txns *PayloadTxns) AssignFromSegments(inSegs []*DecodedTxn) {
    N := len(inSegs)
    last := inSegs[N-1]

    txns.PayloadEncoding = last.Info.PayloadEncoding
    txns.PayloadID       = last.Info.PayloadID
    txns.NewlyAuthored   = false

    if cap(txns.Txns) < N {
        txns.Txns = make([]RawTxn, N)
    } else {
        txns.Txns = txns.Txns[:N]
    }

    for i, seg := range inSegs {
        txns.Txns[i].URID  = seg.Info.URID
        txns.Txns[i].Bytes = seg.RawTxn
    }

}

// AssignFromMonoSegment assigns this payload txn set from a txn that has only one segment
func (txns *PayloadTxns) AssignFromMonoSegment(seg *DecodedTxn) {
    txns.PayloadEncoding = seg.Info.PayloadEncoding
    txns.PayloadID       = seg.Info.PayloadID
    txns.NewlyAuthored   = false

    txns.Txns = append(txns.Txns[:0], RawTxn{
        Bytes: seg.RawTxn,
        URID: seg.Info.URID,
    })
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
    seg      *DecodedTxn    // if set, this is a seg waiting to be claimed 
    segGroup *segGroup     // if set, this is the segGroup waiting for this seg
}

type segGroup struct {
	SegsPresent uint32
	Segs        []*DecodedTxn
	Info        TxnInfo
}

// NewTxnCollater creates a new TxnCollater
func NewTxnCollater() TxnCollater {
	return TxnCollater{
		segMap: make(map[URIDBlob]segEntry),
	}
}

// MergeSegment adds the given txn to this segment group
func (group *segGroup) MergeSegment(seg *DecodedTxn) bool {

	//segInfo := seg.TxnInfo
	idx := seg.Info.SegIndex

	if group.Segs[idx] == nil {
		group.SegsPresent++
	}
	group.Segs[idx] = seg
	
    return group.SegsPresent == group.Info.SegTotal
}

// DecodeAndCollateTxn decodes the given txn and attempts to "collate" it, meaning 
//    any payloads that are completed get a callback via PayloadHandler()
func (tc *TxnCollater) DecodeAndCollateTxn(
    inDecoder TxnDecoder,
    inRawTxn *RawTxn,
) error {

    // TODO: use sync.Pool
    seg := &DecodedTxn{
        RawTxn: inRawTxn.Bytes,
    }

    err := seg.DecodeRawTxn(inDecoder)
    if err != nil {
        return err
    }

    if len(seg.Info.URID) < URIDSz {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "invalid txn URID") 
    }

    if len(inRawTxn.URID) > 0 && ! bytes.Equal(seg.Info.URID, inRawTxn.URID) {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "txn URID did not match label") 
    }

    return tc.Desegment(seg)
}
 

// Consolidate collates multisegment txns until all the segments are present.
//
// If txnIn completes this multi-segment txn group, the txnIncoming returned is a reconstructed (single) segment ready for decapsulation.
func (tc *TxnCollater) consolidate(group *segGroup) error {

	N := group.Info.SegTotal

	// Exit if there's still more segments to go
	if missing := N - group.SegsPresent; missing > 0 {
		return plan.Errorf(nil, plan.TxnNotConsistent, "missing %d out of %d segments", missing, N )
	}

	totalSz := uint32(0)
	var prevURID []byte

	// First verify all segments present and calc size
	for _, seg := range group.Segs {

        if seg.Info.SegTotal != group.Info.SegTotal || seg.Info.PayloadEncoding != group.Info.PayloadEncoding {
            return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v failed group consistency check", seg.URIDstring())
        } else if ! bytes.Equal(seg.Info.PayloadID, group.Info.PayloadID) {
            return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expected payload ID %v, got %v", seg.URIDstring(), group.Info.PayloadID, seg.Info.PayloadID)
        } else if ! bytes.Equal(seg.Info.PrevURID, prevURID) {
			return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expects prev seg URID %v, got %v", seg.URIDstring(), seg.Info.PrevURID, prevURID)
		}

		totalSz += seg.Info.SegSz

		prevURID = seg.Info.URID
	}

    txns := &PayloadTxns{}
    txns.AssignFromSegments(group.Segs)

	// Reassemble the segment data into a single virutal segment
    payload := make([]byte, totalSz)
    pos := uint32(0)
	for _, seg := range group.Segs {
		segEnd := pos + seg.Info.SegSz
		copy(payload[pos:segEnd], seg.PayloadSeg)
		pos = segEnd
	}

    return tc.PayloadHandler(payload, txns)
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
        txns := &PayloadTxns{}
        txns.AssignFromMonoSegment(seg)
        err = tc.PayloadHandler(seg.PayloadSeg, txns)
	} else {
        
        txnURID := segURID.Blob()
        entry := tc.segMap[txnURID]

        // Use the last segment as a catalyst to start a new group
        if entry.segGroup == nil && seg.Info.SegIndex + 1 == seg.Info.SegTotal { 

            entry.segGroup = &segGroup{
                SegsPresent: 0,
                Segs:        make([]*DecodedTxn, seg.Info.SegTotal),
                Info:        seg.Info,
            }
        }

        entry.seg = seg
        tc.segMap[txnURID] = entry

        // if entry.segGroup is set, that means the given group is waiting on this segment
        if entry.segGroup != nil {
            group := entry.segGroup

            // Advance the leading "edge" of the segGroup towards index 0 (the first segment in the group)
            for entry.seg != nil && err == nil {
        
                canConsolidate := group.MergeSegment(entry.seg)
                if canConsolidate {

                    // Detach each txn segment
                    for _, seg := range group.Segs {
                        delete(tc.segMap, seg.TxnURID().Blob())
                    }

                    err = tc.consolidate(group)
                    break
                }

                if err != nil || entry.seg.Info.SegIndex == 0 {
                    break
                }

                prevURID := URID(entry.seg.Info.PrevURID).Blob()
                prev := tc.segMap[prevURID]
                prev.segGroup = group
                tc.segMap[prevURID] = prev

                entry = prev
            }
        }
	}

	return err
}
