package repo

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	//"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/ctx"

	"github.com/dgraph-io/badger/v2"
)

// LID is the local assigned ID, an integer assigned the given channel for brevity and look
type LID uint32

// domain is the top level interface for a community's channels.
type domain struct {
	ctx.Context

	domainName      string
	host            *host
	stateDB         *badger.DB
	chSessMu        sync.RWMutex
	chSess          map[string]*chSess
	txsToMerge      chan *Tx
	txsToDecode     chan *RawTx
	writeScrap      []byte
	chAutoStopDelay time.Duration
}

type chSess struct {
	ctx.Context

	//LID       LID
	stateDB      *badger.DB
	domain       *domain
	ChID         string
	keyPrefix    ChKey
	keyPrefixBuf [255]byte
	subs         []*chSub
	subsMu       sync.RWMutex
}

type chSub struct {
	ctx.Context

	chSess     *chSess
	chReq      *ChReq
	nodeOutbox chan *Node
	txInbox    chan *Tx
}

func newDomain(
	domainName string,
	host *host,
) *domain {

	d := &domain{
		domainName:      domainName,
		stateDB:         host.stateDB,
		host:            host,
		chSess:          make(map[string]*chSess),
		writeScrap:      make([]byte, 32000),
		chAutoStopDelay: 60 * time.Second,
	}

	d.SetLogLabel(domainName)

	return d
}

// Start -- see interface Domain
//
// Ctx organization:
// [host]
//     [domain1]
//         [chSess ABC]
//             [chSub003]
//             [chSub019]
//             [chSub020]
//         [chSess XYZ]
//             ...
//     [domain2]
//         ...
func (d *domain) Start() error {
	err := d.CtxStart(
		d.ctxStartup,
		nil,
		d.onChSessStopping,
		d.ctxStopping,
	)
	return err
}

func (d *domain) ctxStartup() error {

	d.Infof(1, "starting")

	//
	//
	//
	//
	// inbound Tx to merge
	//
	d.txsToMerge = make(chan *Tx, 1)
	d.CtxGo(func() {

		for tx := range d.txsToMerge {
			chSess, err := d.getChSess(tx.TxOp.ChStateURI.ChID, true)
			if chSess != nil {
				err = chSess.mergeTx(tx)
			}
			if err != nil {
				d.Warnf("merge error for Tx %v: %v", TID(tx.TID).SuffixStr(), err)
				continue
			}

			chSess.broadcastToSubs(tx)
		}
		d.Info(1, "shutdown complete")
	})
	//
	//
	//
	//
	// inbound RawTx to unpack/decode/decrypt
	//
	// Decodes incoming raw txns and inserts them into the collator, which reassembles txn segments into the original (entries).
	d.txsToDecode = make(chan *RawTx, 1)
	d.CtxGo(func() {

		// TODO: choose different txn decoder based on spInfo
		//txnDecoder := ds.NewTxnDecoder(false)

		for txIn := range d.txsToDecode {

			d.Warnf("dropping RawTx %v", TID(txIn.TID).SuffixStr())
			// txnSet, err := CR.txnCollater.DecodeAndCollateTxn(txnDecoder, &txnIn)
			// if err != nil {
			//     CR.Warnf("error processing txn %v: %v", txnIn.URID, err)
			// } else if txnSet != nil {
			//     payloadCodec := txnSet.PayloadCodec()

			//     switch payloadCodec {
			//         case plan.EntryCryptCodec:
			//             entry := newChEntry(entryFromStorageProvider)
			//             entry.PayloadTxnSet = txnSet
			//             CR.entriesToMerge <- entry
			//         default:
			//             CR.Warn("encountered unhandled payload multicodec: ", payloadCodec)
			//     }
			// }
		}

		close(d.txsToMerge)
	})

	return nil
}

func (d *domain) onChSessStopping(child ctx.Ctx) {

	if d.CtxChildCount() <= 1 {
		d.CtxGo(func() {
			ticker := time.NewTicker(d.host.domainAutoStopDelay)
			select {
			case <-ticker.C:
				d.host.stopDomainIfIdle(d)
			case <-d.CtxStopping():
				break
			}
			ticker.Stop()
		})
	}
}

func (d *domain) ctxStopping() {

	// Don't we have to wait for the vaultMgr ctx to be done before proceeding?
	// Trick question: it's a child ctx, this ctx won't be given the signal to stop until vaultMgr is stopped.
	close(d.txsToDecode)
}

// DomainName -- see interface Domain
func (d *domain) DomainName() string {
	return d.domainName
}

// func (d *domain) registerSub(sub *chSub) {
//     d.subsMu.Lock()
//     d.subs = append(d.subs, sub)
//     d.subsMu.Unlock()
// }

// func (d *domain) unregisterSub(sub *chSub) {
//     d.subsMu.Lock()
//     for _, existingSub := range d.subs {

//     }
//     d.subs = append(d.subs, sub)
//     d.subsMu.Unlock()
// }

func (d *domain) getChSess(chID string, autoMount bool) (*chSess, error) {
	d.chSessMu.RLock()
	ch := d.chSess[chID]
	d.chSessMu.RUnlock()

	if ch != nil || autoMount == false {
		return ch, nil
	}

	return d.mountChSess(chID)
}

func (d *domain) mountChSess(chID string) (*chSess, error) {
	d.chSessMu.Lock()
	defer d.chSessMu.Unlock()

	ch := d.chSess[chID]
	if ch != nil {
		return ch, nil
	}

	ch = &chSess{
		stateDB: d.stateDB,
		domain:  d,
		ChID:    chID,
	}

	err := ch.CtxStart(
		ch.ctxStartup,
		nil,
		ch.onChSubStopping,
		ch.ctxOnStopping,
	)
	if err != nil {
		return nil, err
	}

	d.chSess[chID] = ch

	// Make the chSess a child ctx of the domain
	d.CtxAddChild(ch, nil)

	return ch, nil
}

func (d *domain) stopChSessIfIdle(ch *chSess) bool {
	d.chSessMu.Lock()
	defer d.chSessMu.Unlock()

	didStop := false

	if d.chSess[ch.ChID] == ch {

		// With the domain's ch session mutex locked, we can reliably call CtxChildCount
		if ch.CtxChildCount() == 0 {
			didStop = ch.CtxStop("idle chSess auto stop", nil)
			delete(d.chSess, ch.ChID)
		}
	}

	return didStop
}

// // getRootKeyForChURI returns the root keypath for a given channel ID.
// func (d *domain) getRootKeyForChURI(uri *ChStateURI, dst ChKey) (ChKey, error) {

// 	if len(uri.DomainName) == 0 {
// 		return nil, ErrCode_InvalidURI.ErrWithMsg("missing domain name")
// 	}

// 	if len(uri.ChID_TID) > 0 {
// 		dst = append(dst[:0], uri.ChID_TID...)
// 		return dst, nil
// 	}

// 	// FUTURE
// 	// TODO: a domain db issue channel ID numbers and then has a separate ch ID/TID lookip where multiple ch ID names remap to a ChID # (a uint32).
// 	// The *same* code can be reused to provide domain name alaiasing as the host level!
// 	if len(uri.ChID) > 0 {
// 		dst = append(dst, '/')
// 		dst = append(dst, uri.ChID...)
// 		dst = append(dst, '/')
// 		return dst, nil
// 	}

// 	return nil, ErrCode_InvalidURI.ErrWithMsg("no channel ID or TID given")
// }

// SubmitTx -- see Domain interface
func (d *domain) SubmitTx(tx *Tx) error {
	d.txsToMerge <- tx

	return nil
}

// OpenChSub -- see interface Domain
func (d *domain) OpenChSub(chReq *ChReq) (ChSub, error) {
	ch, err := d.getChSess(chReq.ChStateURI.ChID, true)
	if err != nil {
		return nil, err
	}

	return ch.OpenChSub(chReq)
}

func (ch *chSess) mergeTx(tx *Tx) error {
	var err error
	{
		dbTx := ch.domain.stateDB.NewTransaction(true)

		scrap := ch.domain.writeScrap

		for idx, entry := range tx.TxOp.Entries {

			ch.Debugf("%d/%d writing: '%s'", idx+1, len(tx.TxOp.Entries), entry.Keypath)

			origKeypath := entry.Keypath
			origReqID := entry.ReqID

			{
				dbEntry := &badger.Entry{
					Key: append(append(scrap[:0], ch.keyPrefix...), entry.Keypath...),
				}
				scrap = scrap[len(dbEntry.Key):]

				entry.ReqID = 0
				entry.Keypath = ""

				// Only retain a scrap buffer that isn't wastefully large
				entrySz := entry.Size()
				entryBuf := scrap
				entryUsesScrap := true
				if entrySz > cap(entryBuf) {
					entryBuf = make([]byte, entrySz+1000)
					if entrySz < 500000 {
						ch.domain.writeScrap = entryBuf
						scrap = entryBuf
					} else {
						entryUsesScrap = false
					}
				}
				entrySz, err = entry.MarshalToSizedBuffer(entryBuf[:entrySz])
				dbEntry.Value = entryBuf[:entrySz]
				if entryUsesScrap {
					scrap = scrap[entrySz:]
				}

				ch.Infof(2, "SET: %v", string(dbEntry.Key))
				err = dbTx.SetEntry(dbEntry)
			}

			// Now that we've serialized the entry, restore meta entry fields (they aren't data that is stored)
			entry.Keypath = origKeypath
			entry.ReqID = origReqID
		}

		// TODO: handle errs
		err = dbTx.Commit()
	}

	if err != nil {
		return err
	}

	return err
}

func (ch *chSess) broadcastToSubs(tx *Tx) {
	ch.subsMu.RLock()
	{
		for _, sub := range ch.subs {
			sub.txInbox <- tx
		}
	}
	ch.subsMu.RUnlock()
}

func (ch *chSess) ctxStartup() error {

	if len(ch.domain.domainName) == 0 {
		return ErrCode_InvalidURI.ErrWithMsg("missing domain name")
	}

	if len(ch.ChID) == 0 {
		return ErrCode_InvalidURI.ErrWithMsg("missing channel ID")
	}

	// FUTURE
	// TODO: a domain db issue channel ID numbers and then has a separate ch ID/TID lookip where multiple ch ID names remap to a ChID # (a uint32).
	// The *same* code can be reused to provide domain name alaiasing as the host level!
	// seq, err := db.GetSequence(key, 1000)
	// defer seq.Release()
	// for {
	// num, err := seq.Next()
	// }
	keypath := fmt.Sprintf("%s/%s/", ch.domain.domainName, ch.ChID)
	ch.keyPrefix = append(ch.keyPrefixBuf[:0], keypath...)
	ch.SetLogLabelf("chSess …%v", ch.ChID[len(ch.ChID)-5:])

	ch.Info(2, "starting ", keypath)

	return nil
}

func (ch *chSess) onChSubStopping(child ctx.Ctx) {

	if ch.CtxChildCount() <= 1 {
		ch.CtxGo(func() {
			ticker := time.NewTicker(ch.domain.chAutoStopDelay)
			select {
			case <-ticker.C:
				ch.domain.stopChSessIfIdle(ch)
			case <-ch.CtxStopping():
				break
			}
			ticker.Stop()
		})
	}
}

func (ch *chSess) ctxOnStopping() {
	if N := len(ch.subs); N > 0 {
		ch.Warnf("chSess being stopped with %d active subs", N)
	}
}

// OpenChSub -- see interface Domain
func (ch *chSess) OpenChSub(chReq *ChReq) (ChSub, error) {

	sub := &chSub{
		chSess:     ch,
		chReq:      chReq,
		nodeOutbox: make(chan *Node),
	}

	// Start the subscription as a child ctx of each ch session
	err := sub.CtxStart(
		sub.ctxStartup,
		nil,
		nil,
		sub.ctxStopping,
	)
	if err != nil {
		return nil, err
	}
	ch.CtxAddChild(sub, nil)

	if sub.chReq.GetOp.MaintainSync {
		sub.txInbox = make(chan *Tx, 4)
		ch.registerSub(sub)
	}

	return sub, nil
}

func (ch *chSess) registerSub(sub *chSub) {
	ch.subsMu.Lock()
	ch.subs = append(ch.subs, sub)
	ch.subsMu.Unlock()
}

func (ch *chSess) unregisterSub(remove *chSub) {
	ch.subsMu.Lock()
	N := len(ch.subs)
	for i := 0; i < N; i++ {
		if ch.subs[i] == remove {
			N--
			ch.subs[i] = ch.subs[N]
			ch.subs[N] = nil
			ch.subs = ch.subs[:N]
			break
		}
	}
	ch.subsMu.Unlock()
}

func (sub *chSub) Outbox() <-chan *Node {
	return sub.nodeOutbox
}

func (sub *chSub) Close() {
	sub.CtxStop("ch sub cancelled", nil)
}

func (sub *chSub) ctxStartup() error {
	var err error

	sub.chReq.GetOp.Keypath, err = NormalizeKeypath(sub.chReq.GetOp.Keypath)
	if err != nil {
		return err
	}

	chDesc := sub.chSess.GetLogLabel()
	//sub.SetLogLabelf("%s/%s sub%03x", sub.chSess.GetLogLabel(), sub.chReq.GetOp.Keypath, sub.chReq.ReqID)
	sub.SetLogLabelf("sub%03d …%s/%s", sub.chReq.ReqID, chDesc[len(chDesc)-5:], sub.chReq.GetOp.Keypath)

	sub.CtxGo(func() {

		sub.sendStateToClient()

		if sub.txInbox != nil {
			for {
				sub.nodeOutbox <- sub.chReq.newResponse(NodeOp_ChSyncResume, nil)

				// This blocks until new txs appear or until the sub is stopping
				tx, running := <-sub.txInbox
				if running == false {
					break
				}
				for _, change := range tx.TxOp.Entries {
					sub.processChange(change)
				}
				sub.nodeOutbox <- sub.chReq.newResponse(NodeOp_ChSyncResume, nil)
			}
		}

		close(sub.nodeOutbox)

		sub.CtxStop("sub complete", nil)
	})

	return nil
}

func (sub *chSub) ctxStopping() {
	sub.Info(2, "stopping")

	if sub.txInbox != nil {
		sub.chSess.unregisterSub(sub)
		close(sub.txInbox)
	}
}

func (sub *chSub) processChange(change *Node) {
	scope := sub.chReq.GetOp.Scope

	if strings.HasPrefix(change.Keypath, sub.chReq.GetOp.Keypath) {
		N := len(sub.chReq.GetOp.Keypath)
		subKey := change.Keypath[N:]
		isMatch := false

		if (scope & KeypathScope_EntryAtKeypath) == KeypathScope_EntryAtKeypath {
			isMatch = len(subKey) == 0
		}
		if (scope & (KeypathScope_Shallow | KeypathScope_ShallowAndDeep)) != 0 {
			if len(subKey) > 1 && subKey[0] == '/' {
				if (scope & KeypathScope_Shallow) == KeypathScope_Shallow {
					isMatch = strings.IndexByte(subKey[1:], '/') < 0
				} else {
					isMatch = true
				}
			}
		}

		if isMatch {
			node := sub.chReq.newResponseFromCopy(change)

			sub.Infof(2, "SYNC: %v", node.Keypath)
			sub.nodeOutbox <- node
		}
	}

}

func (sub *chSub) unmarshalAndSend(itemPrefixSkip int, item *badger.Item) error {
	return item.Value(func(entryBuf []byte) error {

		node, err := sub.chReq.newChEntry(entryBuf)
		if err != nil {
			return err
		}

		// TODO: use binary keypaths?
		// node.Keypath = append(node.Keypath[:0], item.Key())
		sub.Infof(2, "GET: %v", string(item.Key()))

		node.Keypath = string(item.Key()[itemPrefixSkip:])
		sub.Infof(2, "GET: %v", node.Keypath)

		sub.nodeOutbox <- node
		return nil
	})
}

func (sub *chSub) sendStateToClient() {
	scope := sub.chReq.GetOp.Scope

	chPrefixLen := len(sub.chSess.keyPrefix)
	opKeypath := append(sub.chSess.keyPrefix, sub.chReq.GetOp.Keypath...)
	readTxn := sub.chSess.stateDB.NewTransaction(false)
	defer readTxn.Discard()

	if (scope & KeypathScope_EntryAtKeypath) == KeypathScope_EntryAtKeypath {
		sub.Infof(2, "GET  EntryAtPath: %v", string(opKeypath))

		item, err := readTxn.Get(opKeypath)
		if err == nil {
			err = sub.unmarshalAndSend(chPrefixLen, item)
		}
		if err != nil && err != badger.ErrKeyNotFound {
			sub.Errorf("failed to read entry %v: %v", string(opKeypath), err)
		}
	}

	if (scope & (KeypathScope_Shallow | KeypathScope_ShallowAndDeep)) != 0 {
		shallowOnly := (scope & KeypathScope_ShallowAndDeep) == 0

		// Add a path sep char to ensure that we don't read items past the entry
		opts := badger.DefaultIteratorOptions
		opts.Prefix = append(opKeypath, '/')
		itr := readTxn.NewIterator(opts)
		defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {

			// If the sub is cancelled, stop son!
			if sub.CtxRunning() == false {
				break
			}

			if shallowOnly && !PathIsShallow(itr.Item().Key(), opKeypath) {
				continue
			}

			err := sub.unmarshalAndSend(chPrefixLen, itr.Item())
			if err != nil {
				sub.Errorf("failed to itr item %v: %v", string(itr.Item().Key()), err)
			}
		}
	}
}

//     go func() {
//     var syncTicker *time.Ticker
//     var syncTickerChan <-chan time.Time

//     // Send a SyncStep soon after we start to tell the client
//     nodesSentThisTick := 1
//     idleTicks := 0

// 	for waiting := true; waiting; {
// 		job.Debugf(">>> SUB STARTED   %v/%v:", job.req.ChURI, getOp.Keypath)

// 		select {

//         // Only send a sync msg after we've sent one or updates.
//         case <-syncTickerChan:
//             if nodesSentThisTick == 0 {
//                 idleTicks++
//                 if idleTicks > 10 {
//                     syncTicker.Stop()
//                     syncTickerChan = nil
//                 }
//             } else {
//                 job.sess.msgOutlet <- job.newResponse(NodeOp_SyncStep)
//                 idleTicks = 0
//                 nodesSentThisTick = 0
//             }

// 		case <-job.canceled():
// 			waiting = false

// 		case rev := <-sub.States(): {
//             changePath := rev.State.Keypath()
//             common := opPath.CommonAncestor(changePath)

//             job.Debugf(">>>  SUB %v/%v", job.req.ChURI, string(changePath))
//             if common.Equals(opPath) {
//                 job.Debugf(">>>  COM %v/%v     common: %v", job.req.ChURI, string(changePath), common)
//                 if  true { //job.filterAndSendNode(rev.State.NodeAt(kNodeKey, nil)) {
//                     nodesSentThisTick++
//                 }
//             }

//             // Make sure the ticker is going once we start sending nodes
//             if nodesSentThisTick == 1 {
//                 if syncTickerChan == nil {
//                     syncTicker = time.NewTicker(time.Millisecond * 200)
//                     syncTickerChan = syncTicker.C
//                 } else {
//                     for len(syncTickerChan) > 0 {
//                         <-syncTickerChan
//                     }
//                 }
//             }
//         } }
// 	}

//     if syncTickerChan != nil {
//         syncTicker.Stop()
//     }

// 	subWait.Done()
// }()

// SplitPath splits path immediately following the final slash, separating it into a directory and file name component.
// If there is no slash in path, Split returns an empty dir and file set to path.
// The returned values have the property that path = dir+file.
func SplitPath(path []byte) (dir, file []byte) {
	i := bytes.LastIndexByte(path, '/')
	return path[:i+1], path[i+1:]
}

// PathIsShallow returns true if the item path's parent is the given parent path
func PathIsShallow(path []byte, mustHaveParent []byte) bool {

	// Skip last char to ignore trailing '/'
	idx := len(path) - 1
	for ; idx >= 0; idx-- {
		if path[idx] == '/' {
			return bytes.Equal(path[:idx], mustHaveParent)
		}
	}

	return len(mustHaveParent) == 0
}

// NormalizeKeypath checks that there are no problems with the given keypath string and returns a standardized Keypath.
//
// This means removing a leading and trailing '/' (if present)
func NormalizeKeypath(keypath string) (string, error) {
	pathLen := len(keypath)

	// Remove leading path sep char
	if pathLen > 0 && keypath[0] == '/' {
		keypath = keypath[1:]
	}

	if pathLen == 0 {
		return "", ErrCode_InvalidKeypath.ErrWithMsg("keypath not set")
	}

	sepIdx := -1
	for i := 0; i <= pathLen; i++ {
		if i == pathLen || keypath[i] == '/' {
			compLen := i - sepIdx - 1
			if compLen == 1 {
				return "", ErrCode_InvalidKeypath.ErrWithMsg("keypath components cannot be a single character")
			} else if compLen == 0 {
				if i < pathLen {
					return "", ErrCode_InvalidKeypath.ErrWithMsg("keypath contains '//'")
				}
			}
			if i < pathLen {
				sepIdx = i
			}
		}
	}

	// Remove trailing path sep char
	if sepIdx == pathLen-1 && pathLen > 0 {
		keypath = keypath[:sepIdx]
	}

	return keypath, nil
}
