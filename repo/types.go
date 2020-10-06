package repo

import (
	"github.com/plan-systems/plan-go/ctx"
)

// TID identifies a specific transaction, community, channel, or channel entry.
type TID []byte

// TIDBuf is the blob version of a TID
type TIDBuf [Const_TIDSz]byte

// NilTxID is a reserved TID that denotes a void/nil/zero value of a TxID
var NilTxID = TID{}

// TxInfo wraps info about a committed transaction
type TxInfo struct {
	TID TIDBuf
}

// TimeFS is the UTC in 1/1<<16 seconds elapsed since Jan 1, 1970 UTC ("FS" = fractional seconds)
//
// timeFS := TimeNowFS()
//
// Shifting this right 16 bits will yield stanard Unix time.
// This means there are 47 bits dedicated for seconds, implying max timestamp of 4.4 million years.
type TimeFS int64

// ChSub returns a stream of requested entries, closing Outbox() when complete (or when Close() is called)
type ChSub interface {
    ctx.Ctx
    
    Outbox() <-chan *ChMsg
    Close()
}

// HostSession wraps a client who has authenticated and session keys.
type HostSession interface {
	EncodeToTxAndSign(txOp *TxOp) (*Tx, error)
}

// Domain is a channel controlled for a family of channels all sharing the same domain.
type Domain interface {
	ctx.Ctx

    // Places this Domain on servce
    Start() error

    // OpenChSub services a channel Get request.
	OpenChSub(chReq *ChReq) (ChSub, error)

    // SubmitTx takes ownership of the given tx and inserts it into the Host pipeline to be validated and merged.
	// Progress of this commit can be monitorted via subscribing to the TID for updates.
	// From this point on, the given Tx should be treated as read-only.
    SubmitTx(tx *Tx) error
    
    // DomainName uniquely identifies this Domain
    DomainName() string
}

// HostSess is a session with a given host
// type HostSess struct {
//     ctx.Context
//     Host Host
//     ReqInbox chan<- *ChReq
//     MsgOutbox <-chan *ChMsg
// }

// Host is the highest level repo controller.  It accepts incoming txns, report status of their processing, and serves channel content.
type Host interface {
	ctx.Ctx

    Domain

    // TODO: see comments in RepoServiceSession()
    NewSession() HostSession

	//SubscribeToTIDUpdates(TID tTID)
}

// NilTID is a reserved TID that denotes a void/nil/zero value of a TID
var NilTID = TIDBuf{}
