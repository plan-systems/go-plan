package datastore


import (
	//"google.golang.org/grpc/encoding"
    log "github.com/sirupsen/logrus"

	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

// dsEncoder implements pdi.TxnEncoder
type dsEncoder struct {
	pdi.TxnEncoder

	SegmentMaxSz uint32

    cryptoKitID  ski.CryptoKitID
	hashKit      ski.HashKit
	author       ski.KeyRef
	skiSession   ski.Session
}

// TxnNameByteLen is the length of txn names used by this agent (and its sister StorageProvider implementation)
//var TxnNameByteLen = 24

// NewTxnEncoder creates a new StorageProviderAgent for use with a pdi-datastore StorageProvider.
// If inSegmentMaxSz == 0, then a default size is chosen
func NewTxnEncoder(
	inSegmentMaxSz uint32,
) (pdi.TxnEncoder, error) {

	defaultKit, perr := ski.NewHashKit(ski.HashKitID_LegacyKeccak_256)
	if perr != nil {
		return nil, perr
	}

	enc := &dsEncoder{
		hashKit:      defaultKit,
		SegmentMaxSz: inSegmentMaxSz,
        cryptoKitID:  txnCryptoKitID,
	}

	if enc.SegmentMaxSz <= 0 {
		enc.SegmentMaxSz = 100 * 1024
	}

    maxSz := uint32(pdi.TxnSegmentMaxSz) - 20000
    if enc.SegmentMaxSz > maxSz {
        enc.SegmentMaxSz = maxSz
    }

	return enc, nil
}

// ResetSigner --see TxnEncoder
func (enc *dsEncoder) ResetSigner(
	inSession ski.Session,
	inFrom    ski.KeyRef,
) error {

	enc.skiSession = inSession
	enc.author = inFrom

	err := enc.checkReady()

	return err
}

func (enc *dsEncoder) checkReady() error {

	if enc.skiSession == nil {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "SKI session missing")
	}

	if len(enc.author.KeyringName) == 0 {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "author/from keyring name missing")
	}

	return nil
}


// EncodeToTxns -- See StorageProviderAgent.EncodeToTxns()
func (enc *dsEncoder) EncodeToTxns(
	inPayload []byte,
	inPayloadCodec pdi.PayloadCodec,
	inTransfers []*pdi.Transfer,
    timeSealed int64,
) ([]pdi.Txn, error) {

	if err := enc.checkReady(); err != nil {
		return nil, err
	}

	segs, err := pdi.SegmentIntoTxns(
		inPayload,
		inPayloadCodec,
		enc.SegmentMaxSz,
	)
	if err != nil {
		return nil, err
	}

	txns := make([]pdi.Txn, len(segs))

    // Put the transfers in the last segment
    segs[len(segs)-1].Transfers = inTransfers

	{
		// Use the same time stamp for the entire batch
		if timeSealed == 0 {
            timeSealed = plan.Now().UnixSecs
        }

		hashKit := enc.hashKit

		signOp := ski.CryptOpArgs{
			CryptOp: ski.CryptOp_SIGN,
			OpKey:   &enc.author,
		}

        pos := uint32(0)
        payloadSz := uint32(len(inPayload))

		for i, seg := range segs {

			if pos + seg.SegSz > payloadSz {
				return nil, plan.Error(nil, plan.AssertFailed, "failed SegInfo payload size check")
			}

            seg.TxnHashname = nil
    		seg.From = enc.author.PubKey
            seg.TimeSealed = timeSealed
            seg.HashKitId = hashKit.HashKitID

            if i > 0 {
                seg.PrevURID = txns[i-1].URID
            }

			// Do one alloc for all out needs.  Add a lil extra for sig and len bytes.
			buf := make([]byte, 200 + seg.Size() + int(seg.SegSz) + pdi.URIDBinarySz + hashKit.HashSz)

			// 1) Append the TxnInfo
			infoLen, merr := seg.MarshalTo(buf[2:])
			if merr != nil {
				return nil, plan.Error(merr, plan.FailedToMarshal, "failed to marshal txnInfo")
			}
			buf[0] = byte(infoLen)
			buf[1] = byte(infoLen >> 8)
			txnLen := uint32(infoLen) + 2

			// 2) Append the payload buf
			copy(buf[txnLen:txnLen + seg.SegSz], inPayload[pos:pos + seg.SegSz])
			txnLen += seg.SegSz
            pos    += seg.SegSz

			// 3) Calc the txn digest
			hashKit.Hasher.Reset()
			hashKit.Hasher.Write(buf[:txnLen])

            // Use the extra we allocated to store the hashname and URID
            j := len(buf) - hashKit.HashSz - pdi.URIDBinarySz
			seg.TxnHashname = hashKit.Hasher.Sum(buf[j:j])
            j += hashKit.HashSz
            seg.URID = pdi.URIDFromInfo(buf[j:j], seg.TimeSealed, seg.TxnHashname)

			if len(seg.TxnHashname) != hashKit.Hasher.Size() {
				return nil, plan.Error(nil, plan.AssertFailed, "hasher returned bad digest length")
			}

			signOp.BufIn = seg.TxnHashname

			sigResults, signErr := enc.skiSession.DoCryptOp(&signOp)
            if signErr != nil {
                return nil, signErr
            }

            // Append the signature
            {
                sig := sigResults.BufOut
                sigLen := uint32(len(sig))
                copy(buf[txnLen:], sig)
                txnLen += sigLen

                // Append the sig length (2 bytes)
                for j := 0; j < 2; j++ {
                    buf[txnLen] = byte(sigLen)
                    sigLen >>= 8
                    txnLen++
                }
            }

            txns[i].URID = seg.URID
            txns[i].RawTxn = buf[:txnLen]
		}

        if pos != payloadSz {
			return nil, plan.Error(nil, plan.AssertFailed, "payloadSz chk failed")
		}
	}

	return txns, nil
}
