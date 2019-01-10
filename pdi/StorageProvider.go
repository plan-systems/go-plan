package pdi

import (
	//"fmt"
    //"sync"

    //"google.golang.org/grpc/encoding"

	//"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

/**********************************************************************************************************************
    StorageProvider wraps a persistent storage service "producer". 

  
    See the service defintion for StorageProvider, located in go-plan/pdi/pdi.proto.

    The StorageProvider+Agent model is designed to wrap ANY kind of append-only database, 
        including a distributed ledger. 
        
        
    The general flow of posting an pdi.EntryCrypt:

        1. Use the agent associated with the target StorageProvider:
                agent := pdi.GetAgentByAgentStr(sp.AgentStr())

        2. Encode and sign the finalized pdi.EntryCrypt to txns native to the target StorageProvider:
                txns := agent.EncodeToNativeTxns(TxnCodec_PbEntryCrypt, entry.Marshal(), activeSPKey)

        3. Submit the signed native txns to the StorageProvider:
                for i := range txns {
                    spService.CommitTxn(txns[i].RawTxn)
                }
*/






// StorageProviderAgent encodes and decodes storage txns native to a specific remote StorageProvider.  
// For each StorageProvider implementation, there is a corresponding StorageProviderAgent that resides locally 
//     on a PLAN community "repo" node, allowing native txns to be generated localled for submission to a 
//     compatible remote StorageProvider.
// The StorageProvider+Agent system preserves the property that a StorageProvider must operate deterministically, 
//     and can only validate txns and maintain a ledger of which public keys can post (and how much).
// StorageProviderAgent is NOT assumed to be threadsafes unless specified otherswise 
type StorageProviderAgent interface {

    // AgentStr is human-readable string that communicates to a StorageProvider what agent encoded a txn.
    // THREADSAFE
    AgentStr() string

    // Encodes the payload and payload codec into one or more native and signed StorageProvider txns.
    EncodeToTxns(
        inPayload     []byte, 
        inPayloadName []byte,
        inCodec       PayloadCodec, 
        inSigner      ski.Session,
        inFrom       *ski.PubKey,
        inCommunityID []byte,
    ) ([]*Txn, error)

    // Decodes a raw txn to/from the StorageProvider for this agent
    DecodeRawTxn(
        inRawTxn   []byte,      // Raw txn to be decoded
        outInfo    *TxnInfo,    // If non-nil, populated w/ info extracted from inTxn
        outSegment *TxnSegment, // If non-nil, populated w/ the segment data from inTxn 
    ) error


    // Generates a txn that transfers the given amount of gas.
    //EncodeTransfer(from ski.PubKey, to ski.PubKey, gasUnits int64) (*Txn, error)

    // Generates a txn that destroys the given address from committing any further txns.
    //EncodeDestruct(from ski.PubKey) (*Txn, error)
}





/*
func DecodeTxns(
    agent StorageProviderAgent,
    inTxns []*TxnSegment,
) ([]*Txn, []*TxnSegment) {


}
*/



/*
var storageMsgPool = sync.Pool{
    New: func() interface{} {
        return new(StorageMsg)
    },
}

// RecycleStorageMsg effectively deallocates the item and makes it available for reuse
func RecycleStorageMsg(inMsg *StorageMsg) {
    for _, txn := range inMsg.Txns {
        txn.Body = nil  // TODO: recycle plan.Blocks too
    }
    storageMsgPool.Put(inMsg)
}

// NewStorageMsg allocates a new StorageMsg
func NewStorageMsg() *StorageMsg {

    msg := storageMsgPool.Get().(*StorageMsg)
    if msg == nil {
        msg = &StorageMsg{}
    } else {
        msg.Txns = msg.Txns[:0]
        msg.AlertCode = 0
        msg.AlertMsg = ""
    }

    return msg
}*/