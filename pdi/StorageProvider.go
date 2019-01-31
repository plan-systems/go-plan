package pdi

import (
	//"fmt"
    //"sync"

    //"google.golang.org/grpc/encoding"

	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

/**********************************************************************************************************************
    StorageProvider wraps a persistent storage service provider. 

    See the service defintion for StorageProvider, located in go-plan/pdi/pdi.proto.

    The TxnEncoder/TxnDecoder model is designed to wrap any kind of append-only database, 
        including a distributed ledger. 
        
*/





// TxnEncoder encodes arbitrary data payloads into storage txns native to a specific StorageProvider.  
// For each StorageProvider implementation, there is a corresponding TxnEncoder that allows 
//     native txns to be generated locally for submission to the remote StorageProvider.
// The StorageProvider + txn Encoder/Decoder system preserves the property that a StorageProvider must operate deterministically, 
//     and can only validate txns and maintain a ledger of which public keys can post (and how much).
// TxnEncoder is NOT assumed to be threadsafes unless specified otherswise 
type TxnEncoder interface {

    // ResetSession -- resets the currently set community ID used for EncodeToTxns()
    // This must be called before other calls into TxnEncoder.
    ResetSession(
        inInvocation  string,
        inSession     ski.Session,
        inCommunityID []byte,
    ) *plan.Perror

    // GenerateNewAccount creates the necessary key(s) in the pres-et SKI session and returns a new public 
    //    key (used as an address) able to originate StorageProvider txns via EncodeToTxns().
    // Pre: ResetSession() must be successfully called.
    GenerateNewAccount() (*ski.PubKey, *plan.Perror)

    // ResetAuthorID -- resets the current set public key used to originate (i.e. sign) txns in EncodeToTxns()
    ResetAuthorID(
        inFrom ski.PubKey,
    ) *plan.Perror

    // EncodeToTxns encodes the payload and payload codec into one or more native and signed StorageProvider txns.
    // Pre: ResetSession() must be successfully called.
    EncodeToTxns(
        inPayload      []byte, 
        inPayloadName  []byte,
        inPayloadCodec PayloadCodec, 
        inTransfers    []Transfer, 
    ) ([]*Txn, *plan.Perror)


    // Generates a txn that destroys the given address from committing any further txns.
    //EncodeDestruct(from ski.PubKey) (*Txn, error)
}





// TxnDecoder decodes storage txns native to a specific remote StorageProvider into the original payloads.  
// TxnDecoder is NOT assumed to be threadsafes unless specified otherswise 
type TxnDecoder interface {

    // TxnEncoderInvocation returns a string for use in TxnEncoder.ResetSession()
    TxnEncoderInvocation() string

    // Decodes a raw txn to/from a StorageProvider (from a corresponding TxnEncoderAgent)
    // Also performs signature validation on the given txn, meaning that if no err is returned,
    //    then the txn was indeed signed by outInfo.From.
    DecodeRawTxn(
        inRawTxn   []byte,      // Raw txn to be decoded
        outInfo    *TxnInfo,    // If non-nil, populated w/ info extracted from inTxn
        outSegment *TxnSegment, // If non-nil, populated w/ the segment data from inTxn 
    ) *plan.Perror


}







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