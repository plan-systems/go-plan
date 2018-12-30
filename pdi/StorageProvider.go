package pdi

import (
	//"fmt"

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
                agent := pdi.GetAgentByAgentStr( sp.AgentStr() )

        2. Encode and sign the finalized pdi.EntryCrypt to txns native to the target StorageProvider:
                txns := agent.EncodeToNativeTxns(TxnCodec_PbEntryCrypt, entry.Marshal(), activeSPKey)

        3. Submit the signed native txns to the StorageProvider:
                spService.CommitTxn(txns[i])
*/

// StorageProviderAgent encodes and decodes storage txns native to a specific remote StorageProvider.  
// For each StorageProvider implementation, there is a corresponding StorageProviderAgent that resides locally 
//     on a PLAN community "repo" node, allowing native txns to be generated localled for submission to a 
//     compatible remote StorageProvider.
// The StorageProvider+Agent system preserves the property that a StorageProvider must operate deterministically, 
//     and can only validate txns and maintain a ledger of which public keys can post (and how much).
type StorageProviderAgent interface {

    // AgentStr is human-readable string that communicates to a StorageProvider what agent encoded a txn.
    AgentStr() string

    // Encodes the payload and payload codec into one or more native and signed StorageProvider txns.
    EncodeToTxns(codec TxnCodec, payload []byte, from ski.PubKey) ([]*Txn, error)

    // Decodes a StorageProvider txn into its corresponding TxnSegment needed for payload reconstruction
    DecodeNativeTxn(txn *Txn) (*TxnSegment, error)

    // Generates a txn that transfers the given amount of gas.
    EncodeTransfer(from ski.PubKey, to ski.PubKey, gasUnits int64) (*Txn, error)

    // Generates a txn that destroys the given address from committing any further txns.
    EncodeDestruct(from ski.PubKey) (*Txn, error)
}


