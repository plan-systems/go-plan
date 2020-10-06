package repo

import (
	//"path"
	"fmt"
    "strings"
    "sync"
    

	//"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/ctx"

	"github.com/dgraph-io/badger/v2"
)

// LID is the local assigned ID, an integer assigned the given channel for brevity and look
type LID uint32

// domain is the top level interface for a community's channels.
type domain struct {
	ctx.Context

	domainName  string
	host        *host
	stateDB     *badger.DB
	chSessMu    sync.RWMutex
	chSess      map[string]*chSess
	txsToMerge  chan *Tx
	txsToDecode chan *RawTx
	vaultMgr    *vaultMgr
	writeScrap  []byte
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

	chSess    *chSess
	chReq     *ChReq
	msgOutbox chan *ChMsg
	txInbox   chan *Tx
	//scrap     []byte
}

func newDomain(
	domainName string,
	host *host,
) *domain {

	d := &domain{
		domainName: domainName,
		stateDB:    host.stateDB,
		host:       host,
		chSess:     make(map[string]*chSess),
		writeScrap: make([]byte, 32000),
	}

	d.SetLogLabel(domainName)

	return d
}

// Start -- see interface Domain
//
// Ctx organization:
// [host]
//     [domain1]
//         [chSess abc]
//             [chSub003]
//             [chSub019]
//             [chSub020]
//         [chSess abc]
//             ...
//     [domain2]
//         ...
func (d *domain) Start() error {
	err := d.CtxStart(
		d.ctxStartup,
		nil,
		nil,
		d.ctxStopping,
	)
	return err
}

func (d *domain) ctxStartup() error {
	var err error

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

	d.vaultMgr = newVaultMgr(d)
	err = d.vaultMgr.Start()
	if err != nil {
		return err
	}

	// Making the vault ctx a child ctx of this domain means that it must Stop before the domain ctx will even start stopping
	d.CtxAddChild(d.vaultMgr, nil)

	return nil
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
		nil,
		nil,
	)
	if err != nil {
		return nil, err
	}

	d.chSess[chID] = ch

	// Make the chSess a child ctx of the domain
	d.CtxAddChild(ch, nil)

	return ch, nil
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
	ch.SetLogLabelf("ch %v", string(ch.keyPrefix))

	ch.Info(2, "starting")

	return nil
}

// OpenChSub -- see interface Domain
func (ch *chSess) OpenChSub(chReq *ChReq) (ChSub, error) {

	sub := &chSub{
		chSess:    ch,
		chReq:     chReq,
        msgOutbox: make(chan *ChMsg),
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
            ch.subs =  ch.subs[:N]
            break
        }
    }
    ch.subsMu.Unlock()
}




func (sub *chSub) Outbox() <-chan *ChMsg {
	return sub.msgOutbox
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

	sub.SetLogLabelf("%v sub %02x %v", sub.chSess.GetLogLabel(), sub.chReq.ReqID, sub.chReq.GetOp.Keypath)

	//sub.Infof(2, "chSub starting: %v", string(sub.opKeypath))

	sub.CtxGo(func() {

		sub.sendStateToClient()

		if sub.txInbox != nil {
            for {
                sub.msgOutbox <- sub.chReq.newResponse(ChMsgOp_ChSyncResume, nil)

                // This blocks until new txs appear or until the sub is stopping
                tx, running := <- sub.txInbox
                if running == false {
                    break
                }
                for _, change := range tx.TxOp.Entries {
                    sub.processChange(change)
                }
                sub.msgOutbox <- sub.chReq.newResponse(ChMsgOp_ChSyncResume, nil)
            }
		}

		close(sub.msgOutbox)

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

func (sub *chSub) processChange(change *ChMsg) {
	scope := sub.chReq.GetOp.Scope

    if strings.HasPrefix(change.Keypath, sub.chReq.GetOp.Keypath) {
        N := len(sub.chReq.GetOp.Keypath)
        subKey := change.Keypath[N:]
        var msg *ChMsg

        if (scope & KeypathScope_EntryAtKeypath) == KeypathScope_EntryAtKeypath {
            if len(subKey) == 0 {
                msg = sub.chReq.newResponse(ChMsgOp_ChEntry, nil) 
                msg.Keypath = change.Keypath
            }
        }
    
        if (scope & KeypathScope_ChildEntries) == KeypathScope_ChildEntries {
            if len(subKey) > 0 && subKey[0] == '/' {
                msg = sub.chReq.newResponse(ChMsgOp_ChEntry, nil) 
                msg.Keypath = subKey[1:]
            }
        }

        if msg != nil {
            sub.Infof(2, "SYNC: %v", msg.Keypath)

            msg.LastModified = change.LastModified
            msg.TypeID = change.TypeID
            msg.Label = change.Label
            msg.Str = change.Str
            msg.Int = change.Int
            msg.X1 = change.X1
            msg.X2 = change.X2
            msg.X3 = change.X3

            msg.Attachment = change.Attachment

            sub.msgOutbox <- msg
        }
    }

}


func (sub *chSub) unmarshalAndSend(itemPrefixSkip int, item *badger.Item) error {
	return item.Value(func(entryBuf []byte) error {

		msg, err := sub.chReq.newChEntry(entryBuf)
		if err != nil {
			return err
		}

		// TODO: use binary keypaths?
		// msg.Keypath = append(msg.Keypath[:0], item.Key())
		sub.Infof(2, "GET: %v", string(item.Key()))

		msg.Keypath = string(item.Key()[itemPrefixSkip:])
		sub.Infof(2, "GET: %v", msg.Keypath)

		sub.msgOutbox <- msg
		return nil
	})
}

func (sub *chSub) sendStateToClient() {
	scope := sub.chReq.GetOp.Scope

	opKeypath := append(sub.chSess.keyPrefix, sub.chReq.GetOp.Keypath...)
	readTxn := sub.chSess.stateDB.NewTransaction(false)
	defer readTxn.Discard()

	if (scope & KeypathScope_EntryAtKeypath) == KeypathScope_EntryAtKeypath {
		sub.Infof(2, "GET  EntryAtPath: %v", string(opKeypath))

		item, err := readTxn.Get(opKeypath)
		if err == nil {
			err = sub.unmarshalAndSend(len(sub.chSess.keyPrefix), item)
		}
		if err != nil && err != badger.ErrKeyNotFound {
			sub.Errorf("failed to read entry %v: %v", string(opKeypath), err)
		}
	}

	if (scope & KeypathScope_ChildEntries) == KeypathScope_ChildEntries {

		// Add a path sep char to ensure that we don't read items past the entry
		opts := badger.DefaultIteratorOptions
		opts.Prefix = append(opKeypath, '/')
		prefixLen := len(opts.Prefix)
		itr := readTxn.NewIterator(opts)
        defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {

			// If the sub is cancelled, bail
			if sub.CtxRunning() == false {
				break
			}

			err := sub.unmarshalAndSend(prefixLen, itr.Item())
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
//                 job.sess.msgOutlet <- job.newResponse(ChMsgOp_SyncStep)
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
//                 if  true { //job.filterAndSendNode(rev.State.NodeAt(kChMsgKey, nil)) {
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


// NormalizeKeypath checks that there are no problems with the given keypath string and returns a standardized Keypath.
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
