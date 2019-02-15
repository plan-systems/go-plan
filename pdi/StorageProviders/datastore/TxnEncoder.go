package datastore

import (
	"sync"
	//"google.golang.org/grpc/encoding"

	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

// dsEncoder implements pdi.TxnEncoder
type dsEncoder struct {
	pdi.TxnEncoder

	SegmentMaxSz int32

	encodingDesc string
	hashKit      ski.HashKit
	author       ski.PubKey
	communityID  []byte
	skiSession   ski.Session
}

// TxnNameByteLen is the length of txn names used by this agent (and its sister StorageProvider implementation)
//var TxnNameByteLen = 24

// NewTxnEncoder creates a new StorageProviderAgent for use with a pdi-datastore StorageProvider.
// If inSegmentMaxSz == 0, then a default size is chosen
func NewTxnEncoder(
	inSegmentMaxSz int32,
) (pdi.TxnEncoder, error) {

	defaultKit, perr := ski.NewHashKit(ski.HashKitID_LegacyKeccak_256)
	if perr != nil {
		return nil, perr
	}

	enc := &dsEncoder{
		hashKit:      defaultKit,
		SegmentMaxSz: inSegmentMaxSz,
	}

	if enc.SegmentMaxSz <= 0 {
		enc.SegmentMaxSz = 100 * 1024
	}

    maxSz := int32(pdi.TxnSegmentMaxSz) - 20000
    if enc.SegmentMaxSz > maxSz {
        enc.SegmentMaxSz = maxSz
    }

	return enc, nil
}

// ResetSession --see TxnEncoder
func (enc *dsEncoder) ResetSession(
	inEncodingDesc string,
	inSession ski.Session,
	inCommunityID []byte,
) error {

	if inEncodingDesc != "" && inEncodingDesc != txnEncodingDesc1 {
		return plan.Errorf(nil, plan.IncompatibleStorage, "incompatible storage requested: %s, have: %s", inEncodingDesc, txnEncodingDesc1)
	}

	enc.encodingDesc = inEncodingDesc
	enc.skiSession = inSession
	enc.communityID = inCommunityID

	err := enc.checkReady()

	return err
}

func (enc *dsEncoder) checkReady() error {

	if enc.skiSession == nil {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "SKI session missing")
	}

	if len(enc.communityID) < 4 {
		return plan.Errorf(nil, plan.EncoderSessionNotReady, "community ID missing")
	}

	return nil
}

// GenerateNewAccount -- See TxnEncoder
func (enc *dsEncoder) GenerateNewAccount() (*ski.PubKey, error) {

	if err := enc.checkReady(); err != nil {
		return nil, err
	}

	blocker := make(chan error, 1)

	var newKey *ski.PubKey

	ski.GenerateKeys(
		enc.skiSession,
		enc.communityID,
		[]*ski.PubKey{
			&ski.PubKey{
				KeyType:     ski.KeyType_SIGNING_KEY,
				CryptoKitId: ski.CryptoKitID_NaCl,
				KeyDomain:   ski.KeyDomain_PERSONAL,
			},
		},
		func(inKeys []*ski.KeyEntry, inErr error) {
			if inErr == nil {
				newKey = inKeys[0].CopyToPubKey()
			}

			blocker <- inErr
		},
	)

	if err := <-blocker; err != nil {
		return nil, err
	}

	return newKey, nil
}

func (enc *dsEncoder) ResetAuthorID(
	inFrom ski.PubKey,
) error {

	enc.author = inFrom

	return nil
}

// EncodeToTxns -- See StorageProviderAgent.EncodeToTxns()
func (enc *dsEncoder) EncodeToTxns(
	inPayload []byte,
	inPayloadLabel string,
	inPayloadCodec pdi.PayloadCodec,
	inTransfers []*pdi.Transfer,
    timeSealed int64,
) ([][]byte, error) {

	if err := enc.checkReady(); err != nil {
		return nil, err
	}

	segs, err := pdi.SegmentIntoTxns(
		inPayload,
		inPayloadLabel,
		inPayloadCodec,
		enc.SegmentMaxSz,
	)
	if err != nil {
		return nil, err
	}

	txns := make([][]byte, len(segs))

	var signErr error

    // Put the transfers in the last segment
    segs[len(segs)-1].Transfers = inTransfers

	{
		// Use the same time stamp for the entire batch
		if timeSealed == 0 {
            timeSealed = plan.Now().UnixSecs
        }

		hashKit := enc.hashKit

		signOp := ski.OpArgs{
			OpName:      ski.OpSign,
			OpKeySpec:   enc.author,
			CommunityID: enc.communityID,
		}

		signErrHandle := &signErr
		txnDone := &sync.WaitGroup{}
        pos := int32(0)
        payloadSz := int32(len(inPayload))

		for i, seg := range segs {

			if pos + seg.SegSz > payloadSz {
				return nil, plan.Error(nil, plan.AssertFailed, "failed SegInfo payload size check")
			}

            seg.TxnHashname = nil
    		seg.From = enc.author.Bytes
            seg.TimeSealed = timeSealed
            seg.HashKitId = hashKit.HashKitID

            if i > 0 {
                seg.SegPrev = pdi.FormUTID("", segs[i-1].TimeSealed, segs[i-1].TxnHashname)
            }

			// Add extra for length signature and len bytes
			rawTxn := make([]byte, 500 + seg.Size() + int(seg.SegSz))

			// 1) Append the TxnInfo
			infoLen, merr := seg.MarshalTo(rawTxn[2:])
			if merr != nil {
				return nil, plan.Error(merr, plan.FailedToMarshal, "failed to marshal txnInfo")
			}
			rawTxn[0] = byte((infoLen >> 8) & 0xFF)
			rawTxn[1] = byte(infoLen & 0xFF)
			txnLen := int32(infoLen) + 2

			// 2) Append the payload buf
			copy(rawTxn[txnLen:txnLen + seg.SegSz], inPayload[pos:pos + seg.SegSz])
			txnLen += seg.SegSz
            pos    += seg.SegSz

			// 3) Calc the txn digest
			hashKit.Hasher.Reset()
			hashKit.Hasher.Write(rawTxn[:txnLen])
			seg.TxnHashname = hashKit.Hasher.Sum(nil)

			if len(seg.TxnHashname) != hashKit.Hasher.Size() {
				return nil, plan.Error(nil, plan.AssertFailed, "hasher returned bad digest length")
			}

    		txnDone.Add(1)

			signOp.Msg = seg.TxnHashname
			enc.skiSession.DispatchOp(
				signOp,
				func(inResults *plan.Block, inErr error) {
					if inErr == nil {
						sig := inResults.Content
						sigLen := int32(len(sig))
						copy(rawTxn[txnLen:], sig)
						txnLen += sigLen

						// Append the sig length div 4
						rawTxn[txnLen] = byte(sigLen >> 2)
						txnLen++

						txns[i] = rawTxn[:txnLen]
					}

					if inErr != nil && *signErrHandle == nil {
						*signErrHandle = inErr
					}

					txnDone.Done()
				},
			)

		    // Wait for len(txns) number of results before we're done
		    txnDone.Wait()
		}

        if pos != payloadSz {
			return nil, plan.Error(nil, plan.AssertFailed, "payloadSz chk failed")
		}

	}

	if signErr != nil {
		return nil, signErr
	}

	return txns, nil
}
