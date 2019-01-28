package datastore

import (

    "sync"
    //"google.golang.org/grpc/encoding"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/ski"
    "github.com/plan-systems/go-plan/plan"

)

// TxnNameByteLen is the length of txn names used by this agent (and its sister StorageProvider implementation)
var TxnNameByteLen = 24


// NewAgent creates a new StorageProviderAgent for pdi-datastore. 
// If inSegmentMaxSz == 0, then a default size is chosen
func NewAgent(
    inAgentStr string,
    inSegmentMaxSz int,
) (pdi.StorageProviderAgent, *plan.Perror) {

    defaultKit, _ := ski.NewHashKit(ski.HashKitID_LegacyKeccak_256)

    agent := &Agent{
        encoderHashKit: defaultKit,
        decoderHashKits: map[ski.HashKitID]ski.HashKit{},
        SegmentMaxSz: inSegmentMaxSz,
    }

    if agent.SegmentMaxSz <= 0 {
        agent.SegmentMaxSz = 10000
    }

    agent.agentStr = datastoreAgent

    if inAgentStr != "" && inAgentStr != agent.agentStr {
        return nil, plan.Errorf(nil, plan.IncompatibleAgent, "incompatible agent requested: %s, have: %s", inAgentStr, agent.agentStr)
    }

    return agent, nil
}


// Agent implements pdi.StorageProviderAgent for pdi/StorageProviders/datastore
type Agent struct {
    encoderHashKit      ski.HashKit

    decoderHashKits     map[ski.HashKitID]ski.HashKit

    SegmentMaxSz        int
    agentStr            string

    pdi.StorageProviderAgent

}

const datastoreAgent = "/plan/pdi/agent/datastore:1"

// AgentStr -- See StorageProviderAgent
func (agent *Agent) AgentStr() string {
    return datastoreAgent
}



// EncodeToTxns -- See StorageProviderAgent.EncodeToTxns()
// TODO: Use ski.Signer interface?
func (agent *Agent) EncodeToTxns(
    inPayload      []byte, 
    inPayloadLabel []byte,
    inPayloadCodec pdi.PayloadCodec, 
    inSigner       ski.Session,
    inFrom        *ski.PubKey,
    inCommunityID  []byte,
) ([]*pdi.Txn, *plan.Perror) {

    segs, err := pdi.SegmentIntoTxns(
        inPayload,
        inPayloadLabel,
        inPayloadCodec, 
        agent.SegmentMaxSz)

    if err != nil {
        return nil, err
    }

    txns := make([]*pdi.Txn, len(segs))

    var signErr *plan.Perror

    {
        // Use the same time stamp for the entire batch
        timeSealed := plan.Now().UnixSecs

        hashKit := agent.encoderHashKit

        signOp := ski.OpArgs{
            OpName: ski.OpSign,
            OpKeySpec: *inFrom,
            CommunityID: inCommunityID,
        }
        
        // TODO: redo this sync impl so we don't have to dim the channel to O(N)
        signErrHandle := &signErr
        signingDone := sync.WaitGroup{}
        signingDone.Add(len(txns))

        for i := range txns {
            
            segSz := len(segs[i].SegData)

            if segSz != int(segs[i].SegInfo.SegmentLength) {
                return nil, plan.Error(nil, plan.AssertFailed, "failed SegInfo payload size check")   
            }

            txnInfo := &pdi.TxnInfo{
                SegInfo: segs[i].SegInfo,
                From: inFrom,
                TimeSealed: timeSealed,
                HashKitId: hashKit.HashKitID,
            }
            
            // Add extra for length signature and len bytes
            rawTxn := make([]byte, 500 + txnInfo.Size() + segSz)

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
            txnInfo.TxnDigest = hashKit.Hasher.Sum(nil)

            if len(txnInfo.TxnDigest) != hashKit.Hasher.Size() {
                return nil, plan.Error(nil, plan.AssertFailed, "hasher returned bad digest length")
            }

            signOp.Msg = txnInfo.TxnDigest
            inSigner.DispatchOp( 
                signOp, 
                func (inResults *plan.Block, inErr *plan.Perror) {
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




// DecodeRawTxn -- See StorageProviderAgent.DecodeRawTxn()
 func (agent *Agent) DecodeRawTxn(
    rawTxn     []byte, 
    outInfo    *pdi.TxnInfo,
    outSegment *pdi.TxnSegment,
) *plan.Perror {

    txnLen := len(rawTxn)
    if txnLen < 50 {
        return plan.Errorf(nil, plan.FailedToUnmarshal, "raw txn is too small (txnLen=%v)",txnLen)
    }

    // 1) Unmarshal the txn info
    var txnInfo pdi.TxnInfo
    pos := 2 + (int(rawTxn[0]) >> 8) + int(rawTxn[1])
    merr := txnInfo.Unmarshal(rawTxn[2:pos])
    if merr != nil {
        return plan.Error(merr, plan.FailedToUnmarshal, "failed to unmarshal txnInfo")
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
    sig := rawTxn[txnLen:txnLen+sigLen]

    // 4) Prep the hasher so we can generate a digest 
    hashKit, ok := agent.decoderHashKits[txnInfo.HashKitId]
    if ! ok {
        var perr *plan.Perror
        hashKit, perr = ski.NewHashKit(txnInfo.HashKitId)
        if perr != nil {
            return perr
        }
        agent.decoderHashKits[txnInfo.HashKitId] = hashKit
    }

    // 5) Calculate the digest of the raw txn
    hashKit.Hasher.Reset()
    hashKit.Hasher.Write(rawTxn[:txnLen])
    txnInfo.TxnDigest = hashKit.Hasher.Sum(nil)

    // 6) Verify the sig
    perr := ski.VerifySignatureFrom(sig, txnInfo.TxnDigest, txnInfo.From)
    if perr != nil {
        return perr
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