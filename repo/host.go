package repo

import (
	"path"
	"sync"
	"time"

	//"strings"
	//"sort"
	// crand "crypto/rand"
	// "strings"
	mrand "math/rand"

	"github.com/plan-systems/plan-go/ctx"
	"github.com/plan-systems/plan-go/device"

	"github.com/dgraph-io/badger/v3"
)

// NewHost is the highest level interface for what this repo package is all about
func NewHost(
	params HostParams,
) (Host, error) {

	pn := &host{
		// activeSessions: ctx.NewSessionGroup(),
		// servicePort:    inServicePort,
		params:              params,
		domains:             make(map[string]Domain),
		domainAutoStopDelay: 60 * time.Second,
	}
	pn.SetLogLabel("host")

	var err error
	if params.BasePath, err = device.ExpandAndCheckPath(params.BasePath, true); err != nil {
		return nil, err
	}

	pn.stateDBPathname = path.Join(params.BasePath, "state.db")

	return pn, nil
}

// HostParams provide all the params a Host needs to run from start to end
type HostParams struct {
	DomainName string
	BasePath   string
}

type host struct {
	ctx.Context

	stateDBPathname     string
	stateDB             *badger.DB
	params              HostParams
	txScrap             []byte
	domains             map[string]Domain
	domainsMu           sync.RWMutex
	domainAutoStopDelay time.Duration
	vaultMgr            *vaultMgr
}

type txUpdate struct {
	TID TID
}

type subJob struct {
	TID TID
}

type commitJob struct {
}

// Start -- see interface Host
func (host *host) Start() error {

	err := host.CtxStart(
		host.ctxStartup,
		nil,
		nil,
		host.ctxStopping,
	)

	return err
}

func (host *host) ctxStartup() error {
	var err error

	host.Infof(1, "opening repo at %v", host.stateDBPathname)
	opts := badger.DefaultOptions(host.stateDBPathname)
	opts.Logger = nil
	host.stateDB, err = badger.Open(opts)
	if err != nil {
		return err
	}

	host.vaultMgr = newVaultMgr(host)
	err = host.vaultMgr.Start()
	if err != nil {
		return err
	}

	// Making the vault ctx a child ctx of this domain means that it must Stop before the domain ctx will even start stopping
	host.CtxAddChild(host.vaultMgr, nil)

	return err
}

func (host *host) ctxStopping() {

	// Since domain are child contexts of this host, by the time we're here, they have all finished stopping.
	// All that's left is to close the dbs
	host.stateDB.Close()
	host.stateDB = nil

}

// Start -- see interface Host
func (host *host) DomainName() string {
	return host.params.DomainName
}

// NewSession -- see interface Host
func (host *host) NewSession() MemberSession {
	return &membSess{}
}

// OpenChSub -- see interface Host
func (host *host) OpenChSub(chReq *ChReq) (ChSub, error) {
	uri := chReq.ChStateURI
	if uri == nil || len(uri.DomainName) == 0 {
		return nil, ErrCode_InvalidURI.ErrWithMsg("no domain name given")
	}

	domain, err := host.getDomain(uri.DomainName, true)
	if err != nil {
		return nil, err
	}
	return domain.OpenChSub(chReq)
}

// SubmitTx -- see interface Host
func (host *host) SubmitTx(tx *Tx) error {

	if tx == nil || tx.TxOp == nil {
		return ErrCode_NothingToCommit.ErrWithMsg("missing tx")
	}

	uri := tx.TxOp.ChStateURI

	if uri == nil || len(uri.DomainName) == 0 {
		return ErrCode_InvalidURI.ErrWithMsg("no domain name given")
	}

	var err error
	{
		// Use the same time value each node we're commiting
        timestampFS := device.TimeNowFS()
		for _, entry := range tx.TxOp.Entries {
			entry.Keypath, err = NormalizeKeypath(entry.Keypath)
			if err != nil {
				return err
			}

			switch entry.Op {
			case NodeOp_NodeUpdate:
			case NodeOp_NodeRemove:
			case NodeOp_NodeRemoveAll:
			default:
				err = ErrCode_CommitFailed.ErrWithMsg("unsupported NodeOp for entry")
			}

            if (entry.RevID == 0) {
                entry.RevID = int64(timestampFS)
            }
		}
	}

	domain, err := host.getDomain(uri.DomainName, true)
	if err != nil {
		return err
	}

	err = domain.SubmitTx(tx)
	if err != nil {
		return err
	}

	return nil
}

func (host *host) getDomain(domainName string, autoMount bool) (Domain, error) {
	host.domainsMu.RLock()
	domain := host.domains[domainName]
	host.domainsMu.RUnlock()

	if domain != nil {
		return domain, nil
	}

	if autoMount == false {
		return nil, ErrCode_DomainNotFound.ErrWithMsg(domainName)
	}

	return host.mountDomain(domainName)
}

func (host *host) mountDomain(domainName string) (Domain, error) {
	host.domainsMu.Lock()
	defer host.domainsMu.Unlock()

	domain := host.domains[domainName]
	if domain != nil {
		return domain, nil
	}

	domain = newDomain(domainName, host)
	host.domains[domainName] = domain

	err := domain.Start()
	if err != nil {
		return nil, err
	}
	host.CtxAddChild(domain, nil)

	return domain, nil
}

func (host *host) stopDomainIfIdle(d Domain) bool {
	host.domainsMu.Lock()
	defer host.domainsMu.Unlock()

	didStop := false

	domainName := d.DomainName()
	if host.domains[domainName] == d {
		dctx := d.Ctx()

		// With the domain's ch session mutex locked, we can reliably call CtxChildCount
		if dctx.CtxChildCount() == 0 {
			didStop = dctx.CtxStop("idle domain auto stop", nil)
			delete(host.domains, domainName)
		}
	}

	return didStop
}

type membSess struct {
	//ctx.Context

}

func (ms *membSess) ExpandAccess(access *EnclaveAccess) error {
	return nil
}


func (ms *membSess) EncodeToTxAndSign(txOp *TxOp) (*Tx, error) {

	if txOp == nil {
		return nil, ErrCode_NothingToCommit.ErrWithMsg("missing txOp")
	}

	if len(txOp.Entries) == 0 {
		return nil, ErrCode_NothingToCommit.ErrWithMsg("no entries to commit")
	}

	if txOp.ChannelGenesis == false && len(txOp.ChStateURI.ChID_TID) < 16 {
		return nil, ErrCode_NothingToCommit.ErrWithMsg("invalid ChID (missing TID)")
	}

	//
	// TODO
	//
	// placeholder until tx encoding and signing is
	var TID TIDBuf
	mrand.Read(TID[:])

	tx := &Tx{
		TID:  TID[:],
		TxOp: txOp,
	}

	if txOp.ChannelGenesis {
		// if len(uri.ChID) > 0 {
		// 	return ErrCode_InvalidURI.ErrWithMsg("URI must be a domain name and not be a path")
		// }
		txOp.ChStateURI.ChID_TID = tx.TID
		txOp.ChStateURI.ChID = TID.Base32()
	}

	return tx, nil
}
