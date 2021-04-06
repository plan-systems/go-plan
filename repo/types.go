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

// MemberSession wraps access to a secure enclave, allowing an interface consumer to perform security services (on behalf of a community member).
type MemberSession interface {
	//ctx.Ctx
	
	// ExpandAccess enables access into a secure enclave (for key direct or indirect access).
	// Currently, this is simply a master key that unlocks a key hive so that the user associated with this session can sign newly authored txns and decrypt private traffic.
	// In the future, these are paramters that connect (and drive) a physical keyfob to prompt the user to authenticate (via physical interaction or biometric input).
	ExpandAccess(access *EnclaveAccess) error

	// Accesses the currently set of activated enclaves and attempts to sign a newly authored transaction
	EncodeToTxAndSign(txOp *TxOp) (*Tx, error)
}

// Domain is a channel controlled for a family of channels all sharing the same domain.
type Domain interface {
	ctx.Ctx

	// Places this Domain on service
	Start() error

	// OpenChSub services a channel Get request.
	OpenChSub(chReq *ChReq) (ChSub, error)

	// SubmitTx takes ownership of the given tx and inserts it into the Host pipeline to be validated and merged.
	// If the given Tx is retained, it should be treated as read-only at this point onward.
	SubmitTx(tx *Tx) error

	// DomainName uniquely identifies this Domain
	DomainName() string
}

// Host is the highest level repo controller.  It accepts incoming txns, report status of their processing, and serves channel content.
type Host interface {
	ctx.Ctx

	Domain

	// TODO: see comments in RepoServiceSession()
	NewSession() MemberSession
}
