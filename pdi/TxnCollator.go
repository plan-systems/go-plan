package pdi

import (
	"github.com/plan-systems/go-plan/plan"
)

// DecodedTxn contains the contents of a decoded raw native txn from a StorageProvider
type DecodedTxn struct {
	UTID       string
	Info       TxnInfo
	PayloadSeg []byte
}

// DecodeRawTxn is a convenience function for TxnDecoder.DecodeRawTxn()
func (txn *DecodedTxn) DecodeRawTxn(
	inRawTxn []byte,
	inDecoder TxnDecoder,
) error {

	var err error
	txn.PayloadSeg, err = inDecoder.DecodeRawTxn(inRawTxn, &txn.Info)

	if err != nil {
		return err
	}

	txn.UTID = FormUTID("", txn.Info.TimeSealed, txn.Info.TxnHashname)
	return nil
}

// TxnCollater helps desegment/"collate" txns from multi-segment pieces into a single segment (that can be unmarshalled).
type TxnCollater struct {
	segGroups map[string]*segGroup
}

type segGroup struct {
	Info        TxnInfo
	SegsPresent uint32
	Segs        []*DecodedTxn
}

// NewTxnCollater creates a new TxnCollater
func NewTxnCollater() TxnCollater {
	return TxnCollater{
		segGroups: make(map[string]*segGroup),
	}
}

// MergeSegment adds the given txn to this segment group
func (group *segGroup) MergeSegment(seg *DecodedTxn) error {

	//segInfo := seg.TxnInfo
	idx := seg.Info.SegIndex
	segSz := int32(len(seg.PayloadSeg))

	if group.Info.SegTotal != seg.Info.SegTotal ||
		group.Info.PayloadSz != seg.Info.PayloadSz ||
		group.Info.PayloadCodec != seg.Info.PayloadCodec {

		return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v failed consistency check", seg.UTID)
	} else if idx >= seg.Info.SegTotal {
		return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v seg index exceeds total", seg.UTID)
	} else if segSz != seg.Info.SegSz {
		return plan.Errorf(nil, plan.TxnNotConsistent, "txn %v seg sz non consistent: expected %d, got %d", seg.UTID, seg.Info.SegSz, segSz)
	}

	if group.Segs[idx] == nil {
		group.SegsPresent++
	}

	group.Segs[idx] = seg
	return nil
}

// CollateTxnSegment collates multisegment txns until all the segments are present.
//
// If txnIn completes this multi-segment txn group, the txnIncoming returned is a reconstructed (single) segment ready for decapsulation.
func (group *segGroup) Consolidate() (*DecodedTxn, error) {

	N := group.Info.SegTotal

	// Exit if there's still more segments to go
	if group.SegsPresent < N {
		return nil, nil
	}

	// We know each segment is good to go b/c MergeSegment() checks each seg as it comes in.
	soleBuf := make([]byte, group.Info.PayloadSz)

	prevUTID := ""
	pos := int32(0)

	// First verify all the segments agree
	for idx, seg := range group.Segs {

		posEnd := pos + seg.Info.SegSz

		if seg.Info.SegPrev != prevUTID {
			return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v: expects prev seg UTID %v, got %v", seg.UTID, seg.Info.SegPrev, prevUTID)
		} else if posEnd > group.Info.PayloadSz {
			return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn %v (seg idx=%d of %d) would overrun payload buf", seg.UTID, idx+1, N)
		}

		copy(soleBuf[pos:posEnd], seg.PayloadSeg)

		pos = posEnd
		prevUTID = seg.UTID
	}

	if pos != group.Info.PayloadSz {
		return nil, plan.Errorf(nil, plan.TxnNotConsistent, "txn seg data totaled %d, expected %d", pos, group.Info.PayloadSz)
	}

	// Cannibalize the a segment as our consolidated/unified seg.  We choose the last one b/c the final/last UTID is already set.
	// We know each segment is good to go b/c MergeSegment() checks each seg as it comes in.
	sole := group.Segs[N-1]
	sole.PayloadSeg = soleBuf
	sole.Info.SegSz = pos
	sole.Info.SegIndex = 0
	sole.Info.SegTotal = 1
	sole.Info.SegPrev = ""
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

	segSz := int32(len(seg.PayloadSeg))

	// If there's only a single segment, we can decode immediately.
	if seg.Info.SegTotal < 1 || seg.Info.SegIndex >= seg.Info.SegTotal {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "bad txn %v, segNum=%d TotalSegments=%d", seg.UTID, seg.Info.SegIndex, seg.Info.SegTotal)
	} else if seg.Info.SegSz != segSz {
		err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v bad seg len: expected %d, got %d", seg.UTID, seg.Info.SegSz, segSz)
	} else if seg.Info.SegTotal == 1 {
		if seg.Info.PayloadSz != segSz {
			err = plan.Errorf(nil, plan.TxnNotConsistent, "txn %v bad payload len: expected %d, got %d", seg.UTID, seg.Info.PayloadSz, segSz)
		} else {
			sole = seg
		}
	} else {
		group := tc.segGroups[seg.Info.PayloadLabel]
		if group == nil {
			group = &segGroup{
				SegsPresent: 0,
				Segs:        make([]*DecodedTxn, seg.Info.SegTotal),
				Info:        seg.Info,
			}
			tc.segGroups[group.Info.PayloadLabel] = group
		}

		err = group.MergeSegment(seg)

		if err == nil {
			sole, err = group.Consolidate()
			if sole != nil {
				delete(tc.segGroups, group.Info.PayloadLabel)
			}
		}
	}

	return sole, err
}
