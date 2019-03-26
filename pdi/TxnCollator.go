package pdi

import (
    "bytes"

	"github.com/plan-systems/go-plan/plan"
)

// DecodedTxn contains the contents of a decoded raw native txn from a StorageProvider
type DecodedTxn struct {
    URID       string
    RawTxn     []byte
	Info       TxnInfo
	PayloadSeg []byte

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

    if len(seg.Info.URID) < URIDBinarySz {
        return plan.Errorf(nil, plan.TxnFailedToDecode, "invalid txn URID") 
    }
    
	seg.URID = URID(seg.Info.URID).String()

	return nil
}

// TxnCollater helps desegment/"collate" txns from multi-segment pieces into a single segment (that can be unmarshalled).
type TxnCollater struct {
    segMap     map[string]segEntry
}

type segEntry struct {
    seg      *DecodedTxn    // if set, this is a seg waiting to be claimed 
    segGroup *segGroup     // if set, this is the segGroup waiting for this seg
}

type segGroup struct {
	SegsPresent uint32
	Segs        []*DecodedTxn
	Info        TxnInfo
    URID        string
}

// NewTxnCollater creates a new TxnCollater
func NewTxnCollater() TxnCollater {
	return TxnCollater{
		segMap: make(map[string]segEntry),
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



// CollateTxnSegment collates multisegment txns until all the segments are present.
//
// If txnIn completes this multi-segment txn group, the txnIncoming returned is a reconstructed (single) segment ready for decapsulation.
func (group *segGroup) Consolidate() (*DecodedTxn, error) {

	N := group.Info.SegTotal

	// Exit if there's still more segments to go
	if missing := N - group.SegsPresent; missing > 0 {
		return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: missing %d out of %d segments", group.URID, missing, N )
	}

	totalSz := uint32(0)
	var prevURID []byte

	// First verify all segments present and calc size
	for _, seg := range group.Segs {

        if seg.Info.SegTotal != group.Info.SegTotal || seg.Info.PayloadEncoding != group.Info.PayloadEncoding {
            return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v failed group consistency check", seg.URID)
        } else if ! bytes.Equal(seg.Info.PayloadName, group.Info.PayloadName) {
            return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expected payload name %v, got %v", seg.URID, group.Info.PayloadName, seg.Info.PayloadName)
        } else if ! bytes.Equal(seg.Info.PrevURID, prevURID) {
			return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expects prev seg URID %v, got %v", seg.URID, seg.Info.PrevURID, prevURID)
		}

		totalSz += seg.Info.SegSz

		prevURID = seg.Info.URID
	}


	// We know each segment is good to go b/c MergeSegment() checks each seg as it comes in.
	soleBuf := make([]byte, totalSz)
    pos := uint32(0)

	// Next, assemble the segment data
	for _, seg := range group.Segs {

		segEnd := pos + seg.Info.SegSz
		copy(soleBuf[pos:segEnd], seg.PayloadSeg)
		pos = segEnd
	}

	// Cannibalize the a segment as our consolidated/unified seg.  We choose the last one b/c the final/last URID is already set.
	// We know each segment is good to go b/c MergeSegment() checks each seg as it comes in.
	sole := group.Segs[N-1]
	sole.PayloadSeg = soleBuf
	sole.Info.SegSz = totalSz
	sole.Info.SegIndex = 0
	sole.Info.SegTotal = 1
	sole.Info.PrevURID = nil
	sole.Info.TxnHashname = nil

	group.Segs = group.Segs[:1]
	group.Segs[0] = sole

	return sole, nil
}



// Desegment collects decoded txns (payload segments) and returns ones that are a single segment.
func (tc *TxnCollater) Desegment(seg *DecodedTxn) (*DecodedTxn, error) {

	var (
		sole *DecodedTxn
		err  error
	)
	segSz := uint32(len(seg.PayloadSeg))

	// If there's only a single segment, we can decode immediately.
	if seg.Info.SegTotal < 1 || seg.Info.SegIndex >= seg.Info.SegTotal {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "bad txn %v, SegIndex=%d SegTotal=%d", seg.URID, seg.Info.SegIndex, seg.Info.SegTotal)
    } else if seg.Info.SegSz != segSz {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v bad seg len: expected %d, got %d", seg.URID, seg.Info.SegSz, segSz)
    } else if seg.Info.SegIndex == 0 && len(seg.Info.PrevURID) != 0 {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v has illegal PrevURID", seg.URID)
	} else if seg.Info.SegTotal == 1 {
		sole = seg
	} else {
        
        entry := tc.segMap[seg.URID]

        // Use the last segment as a catalyst to start a new group
        if entry.segGroup == nil && seg.Info.SegIndex + 1 == seg.Info.SegTotal { 

            entry.segGroup = &segGroup{
                SegsPresent: 0,
                Segs:        make([]*DecodedTxn, seg.Info.SegTotal),
                Info:        seg.Info,
                URID:        seg.URID,
            }
        }

        entry.seg = seg
        tc.segMap[seg.URID] = entry

        // if entry.segGroup is set, that means the given group is waiting on this segment
        if entry.segGroup != nil {
            group := entry.segGroup

            // Advance the leading "edge" of the segGroup towards index 0 (the first segment in the group)
            for entry.seg != nil && err == nil {
        
                canConsolidate := group.MergeSegment(entry.seg)
                if canConsolidate {

                    // Cleanup the seg map
                    for _, seg := range group.Segs {
                        delete(tc.segMap, seg.URID)
                    }

                    sole, err = group.Consolidate()
                    break
                }

                if err != nil || entry.seg.Info.SegIndex == 0 {
                    break
                }

                segPrev := URID(entry.seg.Info.PrevURID).String()
                prev := tc.segMap[segPrev]
                prev.segGroup = group
                tc.segMap[segPrev] = prev

                entry = prev
            }
        }
	}

	return sole, err
}
