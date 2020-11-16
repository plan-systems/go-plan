package repo

import (
	"github.com/plan-systems/plan-go/ctx"
)

// TID identifies a specific transaction, community, channel, or channel entry.
type TID []byte

// TIDBuf is the blob version of a TID
type TIDBuf [Const_TIDSz]byte

// ChSub returns a stream of requested entries, closing Outbox() when complete (or when Close() is called)
type ChSub interface {
	ctx.Ctx

	Outbox() <-chan *Node
	Close()
}

// HostSession wraps a client who has authenticated.
type HostSession interface {
	//ctx.Ctx

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

// Host is the highest level repo controller.  It accepts incoming txns, report status of their processing, and serves channel content.
type Host interface {
	ctx.Ctx

	Domain

	// TODO: see comments in RepoServiceSession()
	NewSession() HostSession

	//SubscribeToTIDUpdates(TID tTID)
}
