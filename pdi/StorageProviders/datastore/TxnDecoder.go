package datastore

import (
	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

const (
	txnEncodingDesc1 = "/plan/pdi/encoding/datastore/1"
)

// dsDecoder implements pdi.TxnDecoder
type dsDecoder struct {
	pdi.TxnDecoder

	encodingDesc string
	hashKits     map[ski.HashKitID]ski.HashKit
}

// NewTxnDecoder creates a TxnDecoder for use with pdi-datastore
func NewTxnDecoder() pdi.TxnDecoder {

	dec := &dsDecoder{
		hashKits:     map[ski.HashKitID]ski.HashKit{},
		encodingDesc: txnEncodingDesc1,
	}

	return dec
}

// EncodingDesc -- see TxnDecoder
func (dec *dsDecoder) EncodingDesc() string {
	return dec.encodingDesc
}

// DecodeRawTxn -- See TxnDecoder
func (dec *dsDecoder) DecodeRawTxn(
	rawTxn []byte,
	outInfo *pdi.TxnInfo,
	outSegment *pdi.TxnSegment,
) error {

	txnLen := len(rawTxn)
	if txnLen < 50 {
		return plan.Errorf(nil, plan.FailedToUnmarshal, "raw txn is too small (txnLen=%v)", txnLen)
	}

	// 1) Unmarshal the txn info
	var txnInfo pdi.TxnInfo
	pos := 2 + (int(rawTxn[0]) >> 8) + int(rawTxn[1])
	if err := txnInfo.Unmarshal(rawTxn[2:pos]); err != nil {
		return plan.Error(err, plan.FailedToUnmarshal, "failed to unmarshal txnInfo")
	}
	if txnInfo.SegInfo == nil {
		return plan.Error(nil, plan.TxnPartsMissing, "txn is missing segment info")
	}

	// 2) Extract the payload buf
	end := pos + int(txnInfo.SegInfo.SegmentLength)
	if end > txnLen {
		return plan.Errorf(nil, plan.FailedToUnmarshal, "payload buffer EOS (txnLen=%v, pos=%v, end=%v)", txnLen, pos, end)

	}
	payloadBuf := rawTxn[pos:end]

	// 3) Extract the sig -- the last byte is the sig len div 4
	sigLen := int(rawTxn[txnLen-1]) << 2
	txnLen -= 1 + sigLen
	if txnLen < 10 {
		return plan.Errorf(nil, plan.FailedToUnmarshal, "txn sig len is wrong (txnLen=%v, sigLen=%v)", txnLen, sigLen)
	}
	sig := rawTxn[txnLen : txnLen+sigLen]

	// 4) Prep the hasher so we can generate a digest
	hashKit, ok := dec.hashKits[txnInfo.HashKitId]
	if !ok {
		var err error
		hashKit, err = ski.NewHashKit(txnInfo.HashKitId)
		if err != nil {
			return err
		}
		dec.hashKits[txnInfo.HashKitId] = hashKit
	}

	// 5) Calculate the digest of the raw txn
	hashKit.Hasher.Reset()
	hashKit.Hasher.Write(rawTxn[:txnLen])
	txnInfo.TxnHashname = hashKit.Hasher.Sum(nil)

	// 6) Verify the sig
	pubKey := &ski.PubKey{
		KeyDomain: ski.KeyDomain_PERSONAL,
		Bytes:     txnInfo.From,
	}
	if err := ski.VerifySignatureFrom(sig, txnInfo.TxnHashname, pubKey); err != nil {
		return err
	}

	if outInfo != nil {
		*outInfo = txnInfo
	}

	if outSegment != nil {
		*outSegment = pdi.TxnSegment{
			SegInfo: txnInfo.SegInfo,
			SegData: payloadBuf,
		}
	}

	return nil
}
