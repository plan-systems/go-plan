package datastore


import (
    //"bytes"
	"sync"

	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)






const (

    // ProtocolDesc appears in StorageEpochs originating from this impl
    ProtocolDesc = "plan/storage/pdi-datastore/1"

    defaultCryptoKit = ski.CryptoKitID_NaCl
    defaultHashKit   = ski.HashKitID_LegacyKeccak_256
)


// dsEncoder implements pdi.TxnEncoder
type dsEncoder struct {
	pdi.TxnEncoder

    threadsafe   bool
    mutex        sync.Mutex

    scrap        []byte
    packer       ski.PayloadPacker
    from         ski.KeyInfo
	SegmentMaxSz uint32
}


// NewTxnEncoder creates a new StorageProviderAgent for use with a pdi-datastore StorageProvider.
// If inSegmentMaxSz == 0, then a default size is chosen
func NewTxnEncoder(
    inMakeThreadsafe bool,
	inSegmentMaxSz uint32,
) pdi.TxnEncoder {

	if inSegmentMaxSz <= 0 {
		inSegmentMaxSz = 100 * 1024
	}

    maxSz := uint32(pdi.TxnSegmentMaxSz) - 20000
    if inSegmentMaxSz > maxSz {
        inSegmentMaxSz = maxSz
    }

	enc := &dsEncoder{
        threadsafe:   inMakeThreadsafe,
        packer:       ski.NewPacker(false),
		SegmentMaxSz: inSegmentMaxSz,
	}

    const maxScrapSz = 80000
    scrapSz := enc.SegmentMaxSz + 2000
    if enc.SegmentMaxSz > maxScrapSz {
        scrapSz = maxScrapSz
    }
    enc.scrap = make([]byte, scrapSz)

	return enc
}


// ResetSigner --see TxnEncoder
func (enc *dsEncoder) ResetSigner(
	inSession ski.Session,
	inFrom    ski.KeyRef,
) error {

	return enc.packer.ResetSigner(
        inSession, 
        inFrom, 
        defaultHashKit,
        &enc.from)

}


// EncodeToTxns -- See StorageProviderAgent.EncodeToTxns()
func (enc *dsEncoder) EncodeToTxns(
	inPayload         []byte,
	inPayloadEnc      plan.Encoding,
	inTransfers       []*pdi.Transfer,
    timeSealed        int64,
) ([]pdi.RawTxn, error) {

	segs, err := pdi.SegmentIntoTxns(
		inPayload,
		inPayloadEnc,
		enc.SegmentMaxSz,
	)
	if err != nil {
		return nil, err
	}

	txns := make([]pdi.RawTxn, len(segs))

    // Put the transfers in the last segment
    segs[len(segs)-1].Transfers = inTransfers

    // Use the same time stamp for the entire batch
    if timeSealed == 0 {
        timeSealed = plan.Now().UnixSecs
    }

    {
        pos := uint32(0)


        // We have a mutex b/c of the shared scrap
        if enc.threadsafe {
            enc.mutex.Lock()
        }

        segBuf := enc.scrap

		for i, seg := range segs {
            
            // Set the rest of the txn fields
            seg.TimeSealed = timeSealed
            if i > 0 {
                seg.PrevUTID = txns[i-1].UTID
            }

            totalSz := 2 + seg.Size() + int(seg.SegSz)
            if totalSz > len(segBuf) {
                segBuf = make([]byte, totalSz)
            }
            totalSz, err = seg.MarshalTo(segBuf[2:])
            if err != nil {
                return nil, plan.Error(nil, plan.MarshalFailed, "failed to marshal txn")
            }
            segBuf[0] = byte(totalSz)
            segBuf[1] = byte(totalSz >> 8)
            totalSz += 2
            copy(segBuf[totalSz:], inPayload[pos : pos + seg.SegSz])
            totalSz += int(seg.SegSz)

            var hash []byte
            txns[i].Bytes, hash, err = enc.packer.SignAndPack(
                segBuf[:totalSz],
                plan.Encoding_TxnPayloadSegment,
                pdi.UTIDBinarySz,
            )
            if err != nil {
                return nil, err
            }

            txns[i].UTID = pdi.UTIDFromInfo(hash, seg.TimeSealed, hash)

            pos += seg.SegSz
        }

        if enc.threadsafe {
            enc.mutex.Unlock()
        }

        if int(pos) != len(inPayload) {
			return nil, plan.Error(nil, plan.AssertFailed, "payload size failed check")
		}
	}

	return txns, nil
}




// dsDecoder implements pdi.TxnDecoder
type dsDecoder struct {
	pdi.TxnDecoder

    unpacker        ski.PayloadUnpacker
}

// NewTxnDecoder creates a TxnDecoder for use with pdi-datastore
func NewTxnDecoder(
    inMakeThreadsafe bool,
) pdi.TxnDecoder {

	return &dsDecoder{
        unpacker: ski.NewUnpacker(inMakeThreadsafe),
	}
}

// DecodeRawTxn -- See TxnDecoder
func (dec *dsDecoder) DecodeRawTxn(
	inRawTxn []byte, 
	outInfo  *pdi.TxnInfo,
) ([]byte, error) {

    out := ski.SignedPayload{
        Hash: make([]byte, 128),
    }

    err := dec.unpacker.UnpackAndVerify(inRawTxn, &out)
    if err != nil {
        return nil, err
    }

    txnLen := uint32(len(out.Payload))
	if txnLen < 20 {
		return nil, plan.Errorf(nil, plan.UnmarshalFailed, "txn is too small (txnLen=%v)", txnLen)
	}

	// 1) Unmarshal the txn info
	var txnInfo pdi.TxnInfo
	pos := 2 + uint32(out.Payload[0]) | (uint32(out.Payload[1]) << 8)
    if pos > txnLen {
		return nil, plan.Error(nil, plan.UnmarshalFailed, "txnInfo len exceeds txn buf size")
    }

	if err := txnInfo.Unmarshal(out.Payload[2:pos]); err != nil {
		return nil, plan.Error(err, plan.UnmarshalFailed, "failed to unmarshal TxnInfo")
	}

    txnInfo.From = out.Signer.PubKey
    txnInfo.TxnHashname = out.Hash
    txnInfo.UTID = pdi.UTIDFromInfo(out.Hash[len(out.Hash):], txnInfo.TimeSealed, out.Hash)

	// 2) Isolate the payload buf
	end := pos + txnInfo.SegSz
	if end > txnLen {
		return nil, plan.Errorf(nil, plan.UnmarshalFailed, "payload buffer EOS (txnLen=%v, pos=%v, end=%v)", txnLen, pos, end)
	}
	txnPayload := out.Payload[pos:end]

	if outInfo != nil {
		*outInfo = txnInfo
	}

	return txnPayload, nil
}
