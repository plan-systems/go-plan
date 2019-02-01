package datastore

import (

    "sync"
    //"google.golang.org/grpc/encoding"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"

)



// dsEncoder implements pdi.TxnEncoder
type dsEncoder struct {
    pdi.TxnEncoder

    SegmentMaxSz        int

    invocation          string

    hashKit             ski.HashKit
    author              ski.PubKey
    communityID         []byte
    skiSession          ski.Session


}


// TxnNameByteLen is the length of txn names used by this agent (and its sister StorageProvider implementation)
//var TxnNameByteLen = 24


// NewTxnEncoder creates a new StorageProviderAgent for use with a pdi-datastore StorageProvider.
// If inSegmentMaxSz == 0, then a default size is chosen
func NewTxnEncoder(
    inSegmentMaxSz int,
) (pdi.TxnEncoder, error) {

    defaultKit, perr := ski.NewHashKit(ski.HashKitID_LegacyKeccak_256)
    if perr != nil {
        return nil, perr
    }

    enc := &dsEncoder{
        hashKit: defaultKit,
        SegmentMaxSz: inSegmentMaxSz,
    }

    if enc.SegmentMaxSz <= 0 {
        enc.SegmentMaxSz = 10000
    }

    return enc, nil
}

// ResetSession --see TxnEncoder
func (enc *dsEncoder) ResetSession(
    inInvocation  string,
    inSession     ski.Session,
    inCommunityID []byte,
) error {

    if inInvocation != "" && inInvocation != txnEncoderInvocation1 {
        return plan.Errorf(nil, plan.IncompatibleStorage, "incompatible storage requested: %s, have: %s", inInvocation, txnEncoderInvocation1)
    }

    enc.invocation = inInvocation
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
func (enc *dsEncoder) GenerateNewAccount(
) (*ski.PubKey, error) {

    if err := enc.checkReady(); err != nil {
        return nil, err
    }

    blocker := make(chan *plan.Err, 1)

    var newKey *ski.PubKey

    ski.GenerateKeys(
        enc.skiSession, 
        enc.communityID, 
        []*ski.PubKey{
            &ski.PubKey{
                KeyType: ski.KeyType_SIGNING_KEY,
                CryptoKitId: ski.CryptoKitID_NaCl,
                KeyDomain: ski.KeyDomain_PERSONAL,
            },
        },
        func(inKeys []*ski.KeyEntry, inErr *plan.Err) {
            if inErr == nil {
                newKey = inKeys[0].CopyToPubKey()
            }

            blocker <- inErr
        },
    )

    if perr := <- blocker; perr != nil {
        return nil, perr
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
// TODO: Use ski.Signer interface?
func (enc *dsEncoder) EncodeToTxns(
    inPayload      []byte, 
    inPayloadLabel []byte,
    inPayloadCodec pdi.PayloadCodec, 
    inTransfers    []*pdi.Transfer, 
) ([]*pdi.Txn, error) {

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

    txns := make([]*pdi.Txn, len(segs))

    var signErr *plan.Err

    {
        // Use the same time stamp for the entire batch
        timeSealed := plan.Now().UnixSecs

        hashKit := enc.hashKit

        signOp := ski.OpArgs{
            OpName: ski.OpSign,
            OpKeySpec: enc.author,
            CommunityID: enc.communityID,
        }
        
        signErrHandle := &signErr
        signingDone := &sync.WaitGroup{}
        signingDone.Add(len(txns))

        for i := range txns {
            
            segSz := len(segs[i].SegData)

            if segSz != int(segs[i].SegInfo.SegmentLength) {
                return nil, plan.Error(nil, plan.AssertFailed, "failed SegInfo payload size check")   
            }

            txnInfo := &pdi.TxnInfo{
                SegInfo: segs[i].SegInfo,
                From: &enc.author,
                TimeSealed: timeSealed,
                Transfers: inTransfers,
                HashKitId: hashKit.HashKitID,
            }
            
            // Add extra for length signature and len bytes
            rawTxn := make([]byte, 500 + txnInfo.Size() + segSz + len(inTransfers) * 200)

            // Only put the transfers in the first txnInfo.
            inTransfers = nil 

            // 1) Append the TxnInfo
            txnLen, merr := txnInfo.MarshalTo(rawTxn[2:])
            if merr != nil {
                return nil, plan.Error(merr, plan.FailedToMarshal, "failed to marshal txnInfo")
            }
            rawTxn[0] = byte((txnLen >> 8) & 0xFF)
            rawTxn[1] = byte(txnLen        & 0xFF)
            txnLen += 2

            // 2) Append the payload buf
            copy(rawTxn[txnLen:txnLen+segSz], segs[i].SegData)
            txnLen += segSz
        
            // 3) Calc the txn digest
            hashKit.Hasher.Reset()
            hashKit.Hasher.Write(rawTxn[:txnLen])
            txnInfo.TxnHashname = hashKit.Hasher.Sum(nil)

            if len(txnInfo.TxnHashname) != hashKit.Hasher.Size() {
                return nil, plan.Error(nil, plan.AssertFailed, "hasher returned bad digest length")
            }

            signOp.Msg = txnInfo.TxnHashname
            enc.skiSession.DispatchOp( 
                signOp, 
                func (inResults *plan.Block, inErr *plan.Err) {
                    if inErr == nil {
                        sig := inResults.Content
                        sigLen := len(sig)
                        copy(rawTxn[txnLen:], sig)
                        txnLen += sigLen

                        // Append the sig length div 4
                        rawTxn[txnLen] = byte(sigLen >> 2)
                        txnLen++

                        txns[i] = &pdi.Txn{
                            TxnInfo: txnInfo,
                            RawTxn: rawTxn[:txnLen],
                        }
                    }

                    if inErr != nil && *signErrHandle == nil {
                        *signErrHandle = inErr
                    }

                    signingDone.Done()
                },
            )
        }

        // Wait for len(txns) number of results before we're done
        signingDone.Wait()
    }

    if signErr != nil {
        return nil, signErr
    }

    return txns, nil
}


