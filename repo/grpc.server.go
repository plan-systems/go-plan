package repo

import (
	"context"
	"fmt"
	"net"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//"path"
	//mrand "math/rand"
	//"strconv"
	//"sync"
	//"time"

	//proto "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/ctx"

	        "github.com/plan-systems/redwood-go/tree"
	rw      "github.com/plan-systems/redwood-go"
	rwtypes "github.com/plan-systems/redwood-go/types"
)



/*
   For a given stateURI:

        any/path/parent/Node567/<fieldByteID>/<nodeFieldKey>  => (field value)
                               /childNode123
                               /childNode987

        Given: NodeIDs must be longer than 1 char (allowing easy child node iteration to occur by scanning starting at key "\001\001").

*/

// var (
//     nodeFields    = []byte{1}
//     nodeTags      = []byte{2}
//     nodeSpaceRoot = []byte{2}
//     nodeAttribs   = []byte{4}
//     nodeBlock     = []byte{5}
// )

func formNodePath(parentPath string, nodeID string) tree.Keypath {
	k := make(tree.Keypath, 0, 192)
	k = append(k, tree.Keypath(parentPath)...)
	k = append(k, tree.Keypath(nodeID)...)
	return k
}

// GrpcServer is the GRPC implementation of repo.proto
type GrpcServer struct {
	ctx.Context

	pipeCount int32
	//pipeSess      []*pipeSess
	host          rw.Host
	server        *grpc.Server
	listenNetwork string
	listenAddr    string
}

// NewGrpcServer creates a new GrpcServer
func NewGrpcServer(host rw.Host, listenNetwork string, listenAddr string) *GrpcServer {
	return &GrpcServer{
		host:          host,
		listenNetwork: listenNetwork,
		listenAddr:    listenAddr,
	}
}

// Start starts this GrpcServer, doi
func (srv *GrpcServer) Start() error {
	return srv.CtxStart(
		func() error { // on startup
			srv.SetLogLabel("grpc")
			srv.Infof(0, "starting grpc service on \x1b[1;32m%v %v\x1b[0m", srv.listenNetwork, srv.listenAddr)

			srv.server = grpc.NewServer(
				grpc.StreamInterceptor(srv.StreamServerInterceptor()),
				grpc.UnaryInterceptor(srv.UnaryServerInterceptor()),
			)
			RegisterRepoGrpcServer(srv.server, srv)

			lis, err := net.Listen(srv.listenNetwork, srv.listenAddr)
			if err != nil {
				return errors.Errorf("failed to listen: %v", err)
			}
			go func() { srv.server.Serve(lis) }()

			return nil
		},
		func() {
			srv.Info(0, "initiating graceful stop")

			// This closes the net.Listener as well.
			go srv.server.GracefulStop()
		},
		nil,
		func() {
			// on shutdown -- block until complete
			srv.server.GracefulStop()
			srv.Info(0, "graceful stop complete")
		},
	)
}

type strMap = map[string]interface{}

type reqJob struct {
	req       *ChReq
	sess      *pipeSess
	filters   nodeFilters
	scrap     []byte
	ctx       context.Context
	ctxCancel context.CancelFunc
}

// Debugf prints output to the output log
func (job *reqJob) Debugf(msgFormat string, msgArgs ...interface{}) {
	job.sess.Infof(2, msgFormat, msgArgs...)
}

// cancelled returns true if this job should back out of all work.
func (job *reqJob) isCancelled() bool {
	select {
	case <-job.ctx.Done():
		return true
	default:
	}
	return false
}

func (job *reqJob) cancelled() <-chan struct{} {
	return job.ctx.Done()
}

func (job *reqJob) Ctx() context.Context {
	return job.ctx
}

func (job *reqJob) exeGet() error {
	srv := job.sess.srv

	var sub rw.StateSubscription
	var err error

	getOp := job.req.GetOp
	scope := job.req.GetOp.Scope

	// Subscribe *before* we start our get or we could miss a state while we're reading, son.
	if job.req.GetOp.MaintainSync {
		sub, err = srv.host.SubscribeStates(job.Ctx(), job.req.ChURI)
		if err != nil {
			return ErrCode_FailedToOpenChURI.Wrap(err)
		}
    }
    defer func(){
        if sub != nil {
            sub.Close()
        }
    }()

    opPath := tree.MakeKeypath(getOp.Keypath, false)

    {
        state, err := srv.host.Controllers().StateAtVersion(job.req.ChURI, nil)
        if err != nil {
            return ErrCode_FailedToOpenChURI.Wrap(err)
        }
        defer state.Close()

        if (scope & KeypathScope_EntryAtKeypath) == KeypathScope_EntryAtKeypath {
            job.Debugf("Get: '%v'", string(opPath))
            job.filterAndSendNode(state.NodeAt(opPath.Push(chMsgKey), nil))
        }

        if (scope & KeypathScope_ChildEntries) == KeypathScope_ChildEntries {
            var iter tree.Iterator
            if (scope & KeypathScope_AllSubEntries) == KeypathScope_AllSubEntries {
                iter = state.DepthFirstIterator(opPath, false, 0)
            } else {
                iter = state.ChildIterator(opPath, false, 0)
            }
            defer iter.Close()

            // @@TODO: this can be accelerated by a special iterator that skips entries with keys shorter than one char.
            for iter.Rewind(); iter.Valid(); iter.Next() {
                if job.isCancelled() {
                    break
                }

                // If the item has a length of 1, it means it's a node field, not a node.
                nodePath := iter.Node().Keypath()
                job.Debugf("Get: iter '%v'", string(nodePath))
                _, leafKey := nodePath.Split()
                if len(leafKey) == 1 && leafKey[0] == NodeChMsgKey {
                    job.filterAndSendNode(state.NodeAt(nodePath, nil))
                }
            }
        }
    }

	if sub != nil {
		var subWait sync.WaitGroup
		subWait.Add(1)

        // Send an initial SyncStep to tell the client to start processing entries
        job.sess.msgOutlet <- job.newResponse(ChMsgOp_SyncStep)

		go func() {
            var syncTicker *time.Ticker
            var syncTickerChan <-chan time.Time

            // Send a SyncStep soon after we start to tell the client
            nodesSentThisTick := 1
            idleTicks := 0

			for waiting := true; waiting; {
				job.Debugf(">>> SUB STARTED   %v/%v:", job.req.ChURI, getOp.Keypath)

				select {

                // Only send a sync msg after we've sent one or updates.
                case <-syncTickerChan:
                    if nodesSentThisTick == 0 {
                        idleTicks++
                        if idleTicks > 10 {
                            syncTicker.Stop()
                            syncTickerChan = nil
                        }
                    } else {
                        job.sess.msgOutlet <- job.newResponse(ChMsgOp_SyncStep)
                        idleTicks = 0
                        nodesSentThisTick = 0
                    }

				case <-job.cancelled():
					waiting = false

				case rev := <-sub.States(): {
                    changePath := rev.State.Keypath()
                    common := opPath.CommonAncestor(changePath)

                    job.Debugf(">>>  SUB %v/%v", job.req.ChURI, string(changePath))
                    if common.Equals(opPath) {
                        job.Debugf(">>>  COM %v/%v     common: %v", job.req.ChURI, string(changePath), common)
                        if job.filterAndSendNode(rev.State.NodeAt(chMsgKey, nil)) {
                            nodesSentThisTick++
                        }
                    }

                    // Make sure the ticker is going once we start sending nodes
                    if nodesSentThisTick == 1 {
                        if syncTickerChan == nil {
                            syncTicker = time.NewTicker(time.Millisecond * 200)
                            syncTickerChan = syncTicker.C
                        } else {
                            for len(syncTickerChan) > 0 {
                                <-syncTickerChan
                            }
                        }
                    }
                } }
			}

            if syncTickerChan != nil {
                syncTicker.Stop()
            }

			subWait.Done()
        }()
        
		// Wait for subscriptions to complete
		subWait.Wait()

        job.Debugf(">>> SUB COMPLETE %v/%v:", job.req.ChURI, getOp.Keypath)
    }

	return nil
}

// 	sub, err := srv.host.SubscribeStates(rpc.Context(), req.ChURI)
// 	if err != nil {
// 		return err
// 	}
// 	defer sub.Close()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()

// 		scanning := true
// 		for scanning {
// 			srv.Debugf(">>> SUB STARTED   %v/%v:", req.ChURI, string(req.NodePathname))

// 			select {
// 			case <-srv.Ctx().Done():
// 				scanning = false
// 			case <-outlet.Context().Done():
// 				srv.Debugf(">>> SUB CANCELLED %v/%v: ", req.ChURI, string(req.NodePathname))
// 				scanning = false
// 			case msg := <-sub.States():
// 				srv.Debugf(">>>  SUB %v/%v: '%v'", req.ChURI, string(req.NodePathname), string(msg.State.Keypath()))
// 				// State  tree.Node
// 				// Leaves []types.ID
// 				// msg.
// 			}
// 		}

// 	}()

// 	srv.Debugf(">>> SUB COMPLETE %v/%v:", req.ChURI, string(req.NodePathname))

// }()

func (job *reqJob) exeTxOp() (*ChMsg, error) {
	putOp := job.req.PutOp

	if len(putOp.Entries) == 0 {
		return nil, ErrCode_NothingToCommit.ErrWithMsg("no entries to commit")
	}

	tx := rw.Tx{
		ID:      rwtypes.RandomID(),
		Patches: make([]rw.Patch, 0, len(putOp.Entries)+1),
	}

	srv := job.sess.srv

	if putOp.ChannelGenesis {

		if strings.ContainsRune(job.req.ChURI, '/') {
			return nil, ErrCode_InvalidURI.ErrWithMsg("URI must be a domain name and not be a path")
		}

		if len(job.req.ChURI) <= 3 {
			return nil, ErrCode_InvalidURI.ErrWithMsgf("URI domain name '%v' is too short", job.req.ChURI)
		}

		// This will all change after redwood (where a txID derives from the hash of the signed tx)
		chIDStr := bufs.Base64Encoding.EncodeToString(tx.ID[len(tx.ID)-int(Const_ChIDSz):])
		job.req.ChURI = path.Join(job.req.ChURI, chIDStr)
		tx.ID = rw.GenesisTxID

		tx.Patches = append(tx.Patches, rw.Patch{
			Keypath: nil,
			Range:   nil,
			Val: strMap{
				"Schema": "plan-systems.org/pnode",
				"Merge-Type": strMap{
					"Content-Type": "resolver/dumb",
					"value":        strMap{},
				},
				"Validator": strMap{
					"Content-Type": "validator/permissions",
					"value": strMap{
						srv.host.Address().String(): strMap{
							"^.*$": strMap{
								"write": true,
							},
						},
					},
				},
			},
		})
	}

	tx.StateURI = job.req.ChURI

	// tx.StateURI = chGenesis.ChURI
	// srv.SendTx(ctx, &tx)
	// tx.ID = rwtypes.RandomID()
	// time.Sleep(10 * time.Second)

	{
		// Use the same time value each node we're commiting
		timeModified := time.Now().Unix()

		for _, entry := range putOp.Entries {

			// TODO -- check that keypath does not contain any single length key comps

			keypath := tree.Keypath(entry.Keypath + NodeChMsgPathKey)
			job.Debugf("Put: '%v'", string(keypath))

			entry.Op = 0
			entry.ReqID = 0
			entry.Keypath = ""
			entry.LastModified = timeModified

			job.scrap = bufs.SmartMarshalToBase64(entry, job.scrap)

			tx.Patches = append(tx.Patches, rw.Patch{
				Keypath: keypath,
				Val:     string(job.scrap),
			})
		}
	}

	err := srv.host.SendTx(job.Ctx(), tx)
	if err != nil {
		return nil, ErrCode_CommitFailed.Wrap(err)
	}

	msg := job.newResponse(ChMsgOp_ReqComplete)
	msg.Attachment = append(msg.Attachment[:0], tx.ID[:]...)
	msg.ValueStr = tx.StateURI

	return msg, nil
}

func (job *reqJob) exeJob() {
	var err error
	var msg *ChMsg

	// Check to see if this req is cancelled before beginning
	if job.isCancelled() {
		err = ErrCode_ReqCancelled.Err()
	} else {

		switch job.req.ReqOp {

		case ChReqOp_Auto:
			switch {
			case job.req.GetOp != nil:
				err = job.exeGet()
			case job.req.PutOp != nil:
				msg, err = job.exeTxOp()
			}

		case ChReqOp_CancelReq:
			err = ErrCode_ReqCancelled.Err()

		default:
			err = ErrCode_UnsupporteReqOp.Err()
		}

		if err == nil && msg == nil {
			if job.isCancelled() {
				err = ErrCode_ReqCancelled.Err()
			}
		}
	}

	if err == nil && msg != nil {
		job.sess.msgOutlet <- msg
	} else {
		job.sendCompletion(err)
	}

	job.sess.removeJob(job.req.ReqID)
}

func (job *reqJob) sendCompletion(err error) {
	var msg *ChMsg
	if err == nil {
		msg = job.newResponse(ChMsgOp_ReqComplete)
	} else {
		msg = job.newResponse(ChMsgOp_ReqDiscarded)
		var reqErr *ReqErr
		if reqErr, _ = err.(*ReqErr); reqErr == nil {
			err = ErrCode_UnnamedErr.Wrap(err)
			reqErr = err.(*ReqErr)
		}
		msg.Attachment = bufs.SmartMarshal(reqErr, msg.Attachment)
	}

	job.sess.msgOutlet <- msg
}


type pipeSess struct {
	ctx.Context

	srv        *GrpcServer
	openReqs   map[int32]*reqJob
	openReqsMu sync.Mutex
	msgOutlet  chan *ChMsg
	scrap      [512]byte
	rpc        RepoGrpc_RepoServicePipeServer
}

func (sess *pipeSess) ctxStartup() error {

	// Send outgoing msgs
	sess.CtxGo(func(ctx.Ctx) {
		for running := true; running; {
			select {
			case msg := <-sess.msgOutlet:
				if msg != nil {
                    sess.rpc.Send(msg)
                    releaseMsg(msg)
				}
			case <-sess.Ctx().Done():
				sess.Info(2, "sess.Ctx().Done()")
				running = false
				break
			}
		}

		// Keep dropping outgoing msgs until all the jobs are done.
		// The session sends an empty msg each time a job is removed to keep this loop going.
		for sess.numJobsOpen() > 0 {
			<-sess.msgOutlet
		}
	})

	// Process incoming requests
	sess.CtxGo(func(ctx.Ctx) {
		for sess.CtxRunning() {
			reqIn, err := sess.rpc.Recv()
			if err != nil {
				sess.Warnf("rpc.Recv() err: %v", err)
				err = sess.rpc.Context().Err()
				if err != nil {
					sess.Warnf("rpc.Context().Err(): %v", err)
				}
			}
			if err != nil {
				break
			}

			sess.dispatchReq(reqIn)
		}
	})

	return nil
}


func (sess *pipeSess) lookupJob(reqID int32) *reqJob {
	sess.openReqsMu.Lock()
	job := sess.openReqs[reqID]
	sess.openReqsMu.Unlock()
	return job
}

func (sess *pipeSess) removeJob(reqID int32) {
	sess.openReqsMu.Lock()
	{
		delete(sess.openReqs, reqID)

		// Send an empty msg to wake up pipe shutdown
		sess.msgOutlet <- nil
	}
	sess.openReqsMu.Unlock()
}

func (sess *pipeSess) numJobsOpen() int {
	sess.openReqsMu.Lock()
	N := len(sess.openReqs)
	sess.openReqsMu.Unlock()
	return N
}

func (sess *pipeSess) addNewJob(reqIn *ChReq) *reqJob {
	job := &reqJob{
		req:  reqIn,
		sess: sess,
	}

	job.ctx, job.ctxCancel = context.WithCancel(sess.Ctx())

	sess.openReqsMu.Lock()
	sess.openReqs[reqIn.ReqID] = job
	sess.openReqsMu.Unlock()

	return job
}

func (sess *pipeSess) dispatchReq(reqIn *ChReq) {
    var reqErr ReqErr
    
	job := sess.lookupJob(reqIn.ReqID)
	if reqIn.ReqOp == ChReqOp_CancelReq {
		if job != nil {
			job.ctxCancel()
		} else {
			reqErr.Code = ErrCode_ReqIDNotFound
		}
	} else {
		if job != nil {
			sess.Warnf("client sent an ReqID that was already in use (ReqID=%v)", reqIn.ReqID)
		} else {
			job := sess.addNewJob(reqIn)
			go job.exeJob()
		}
	}

	// Sends an error if reqErr.Code was set
	if reqErr.Code != ErrCode_NoErr {
		sz, _ := reqErr.MarshalTo(sess.scrap[:])
		sess.msgOutlet <- &ChMsg{
			Op:         ChMsgOp_ReqDiscarded,
			ReqID:      reqIn.ReqID,
			Attachment: sess.scrap[:sz],
		}
	}
}

func (sess *pipeSess) cancelAll() {
	jobsCancelled := 0
	sess.openReqsMu.Lock()
	for _, job := range sess.openReqs {
		if job.isCancelled() == false {
			jobsCancelled++
			job.ctxCancel()
		}
	}
	sess.openReqsMu.Unlock()
	if jobsCancelled > 0 {
		sess.Infof(1, "cancelled %v jobs", jobsCancelled)
	}
}

// RepoServicePipe is the Grpc session a client opens and keeps open.
// Multiple pipes can be open at any time by the same client or multiple clients.
func (srv *GrpcServer) RepoServicePipe(rpc RepoGrpc_RepoServicePipeServer) error {

	sess := &pipeSess{
		srv:       srv,
		openReqs:  make(map[int32]*reqJob),
		msgOutlet: make(chan *ChMsg, 4),
		rpc:       rpc,
	}

	sess.SetLogLabel(fmt.Sprintf("sess%02d", atomic.AddInt32(&srv.pipeCount, 1)))

	err := sess.CtxStart(
		sess.ctxStartup,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return err
	}

	select {
	case <-srv.Ctx().Done():
		sess.Info(2, "srv.Ctx().Done()")
		sess.cancelAll()
		sess.CtxStop(srv.CtxStopReason(), nil)
	case <-rpc.Context().Done():
		sess.Info(2, "rpc.Context().Done()")
		sess.cancelAll()
		sess.CtxStop("rpc context done", nil)
	}

	sess.CtxWait()

	return nil
}



	// 	sub, err := srv.host.SubscribeStates(rpc.Context(), req.ChURI)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer sub.Close()

	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()

	// 		scanning := true
	// 		for scanning {
	// 			srv.Debugf(">>> SUB STARTED   %v/%v:", req.ChURI, string(req.NodePathname))

	// 			select {
	// 			case <-srv.Ctx().Done():
	// 				scanning = false
	// 			case <-outlet.Context().Done():
	// 				srv.Debugf(">>> SUB CANCELLED %v/%v: ", req.ChURI, string(req.NodePathname))
	// 				scanning = false
	// 			case msg := <-sub.States():
	// 				srv.Debugf(">>>  SUB %v/%v: '%v'", req.ChURI, string(req.NodePathname), string(msg.State.Keypath()))
	// 				// State  tree.Node
	// 				// Leaves []types.ID
	// 				// msg.
	// 			}
	// 		}

	// 	}()

	// 	srv.Debugf(">>> SUB COMPLETE %v/%v:", req.ChURI, string(req.NodePathname))

	// }()

    // Close service session when the server is stopping or when the client side closes the connection/
    

// func (srv *GrpcServer) ChannelGenesis(ctx context.Context, req *ChannelGenesisReq) (*TxInfo, error) {

// func (srv *GrpcServer) ChangeUserPerms(ctx context.Context, req *ChangeUserPermsReq) (*TxInfo, error) {
// 	if req.Address == "" {
// 		return nil, errors.New("no address specified")
// 	}

// 	var writeKeypathsRegex string
// 	if req.GetWriteKeypathsRegex() != "" {
// 		writeKeypathsRegex = req.GetWriteKeypathsRegex()
// 	} else {
// 		writeKeypathsRegex = "^.*$"
// 	}

// 	tx := rw.Tx{
// 		StateURI: req.ChURI,
// 		ID:       rw.GenesisTxID,
// 		Patches: []rw.Patch{{
// 			Keypath: tree.Keypath("Validator/value").Pushs(req.GetAddress()),
// 			Val: strMap{
// 				writeKeypathsRegex: strMap{
// 					"write": req.GetCanWrite(),
// 				},
// 			},
// 		}},
// 	}

// 	return srv.SendTx(ctx, &tx)
// }

// func (srv *GrpcServer) openState(chURI string) (tree.Node, error) {
// 	if chURI == "" {
// 		return nil, errors.New("missing channel/state URI")
// 	}

// 	state, err := srv.host.Controllers().StateAtVersion(chURI, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// if exists, err := state.Exists(tree.Keypath(nodesFork)); err != nil {
// 	// 	srv.Warnf("error fetching fork %v: %v", nodesFork, err)
// 	// 	return nil, err
// 	// } else if exists == false {
// 	// 	return nil, errors.Errorf("fork %v not found", nodesFork)
// 	// }

// 	return state, nil
// }

// // func (srv *GrpcServer) NodeGet(ctx context.Context, req *NodeGetReq) (*Node, error) {
// // 	if len(req.ParentID) <= 1 && req.ParentID != rootNodeID {
// // 		return nil, errors.New("invalid/missing ParentID")
// // 	}

// // 	if len(req.NodeID) <= 1 {
// // 		return nil, errors.New("invalid/missing NodeID")
// // 	}

// // 	state, err := srv.openState(req.ChURI)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	defer state.Close()

// // 	subPath := formNodePath(req.ParentID, req.NodeID)

// // 	// TODO: why isn't Exists() a call variant of NodeAt()??
// // 	exists, err := state.Exists(subPath)
// // 	if err != nil {
// // 		return nil, err
// // 	} else if exists == false {
// // 		return nil, errors.Errorf("node not found #NnF (%v)", subPath)
// // 	}
// // 	node := state.NodeAt(subPath, nil)
// // 	outNode := srv.filterAndExportNode(node, &exportFilters{
// // 		NodeFetchFlags:  req.NodeFetchFlags,
// // 		LinksFetchFlags: req.LinksFetchFlags,
// // 	})
// // 	if outNode == nil {
// // 		return nil, errors.New("error exporting node")
// // 	}

// // 	return outNode, nil
// // }

// var emptyNode = &Node{}

// func (srv *GrpcServer) NodeGett(req *NodeGetReq, outlet RPC_NodeGetServer) error {

// 	if req.LoadFields == NodeFields_ALL_FIELDS {
// 		req.LoadFields = 0xFF
// 	}

// 	filters := nodeFilters{
// 		req:        req,
// 		timeSearch: req.TMax >= req.TMin && (req.TMax != 0 || req.TMin != 0),
// 		// NodeFetchFlags:  req.NodeFetchFlags,
// 		// LinksFetchFlags: req.LinksFetchFlags,
// 		// FilterFlags:     req.FilterFlags,
// 	}

// 	var err error
// 	{
// 		if err == nil && req.RegexNodeID != "" {
// 			filters.regexNodeID, err = regexp.Compile(req.RegexNodeID)
// 			if err != nil {
// 				return errors.Errorf("failed to compile RegexNodeID: '%v' (%v)", req.RegexNodeID, err)
// 			}
// 		}
// 		if err == nil && req.RegexName != "" {
// 			filters.regexName, err = regexp.Compile(req.RegexName)
// 			if err != nil {
// 				return errors.Errorf("failed to compile RegexName: '%v' (%v)", req.RegexName, err)
// 			}
// 		}
// 		if err == nil && req.RegexTags != "" {
// 			filters.regexTags, err = regexp.Compile(req.RegexTags)
// 			if err != nil {
// 				return errors.Errorf("failed to compile RegexTags: '%v' (%v)", req.RegexTags, err)
// 			}
// 		}
// 		if err == nil && req.RegexTypeID != "" {
// 			filters.regexTypeID, err = regexp.Compile(req.RegexTypeID)
// 			if err != nil {
// 				return errors.Errorf("failed to compile RegexTypeID: '%v' (%v)", req.RegexTypeID, err)
// 			}
// 		}
// 	}

//     //srv.Debugf(">>> NodeGet %v/%v", req.ChURI, string(req.NodePathname))

//     // TODO Move to start and put into gofunc so we don't miss any during the above scan
//     if req.Subscribe {

//         // Spec says that node ID's that is empty signals an end to a run of nodes.
//         outlet.Send(emptyNode)

//         var wg sync.WaitGroup

//         sub, err := srv.host.SubscribeStates(outlet.Context(), req.ChURI)
//         if err != nil {
//             return err
//         }
//         defer sub.Close()

//         wg.Add(1)
//         go func() {
//             defer wg.Done()

//             scanning := true
//             for scanning {
//                 srv.Debugf(">>> SUB STARTED   %v/%v:", req.ChURI, string(req.NodePathname))

//                 select {
//                 case <-srv.Ctx().Done():
//                     scanning = false
//                 case <-outlet.Context().Done():
//                     srv.Debugf(">>> SUB CANCELLED %v/%v: ", req.ChURI, string(req.NodePathname))
//                     scanning = false
//                 case msg := <-sub.States():
//                     srv.Debugf(">>>  SUB %v/%v: '%v'", req.ChURI, string(req.NodePathname), string(msg.State.Keypath()))
//                     // State  tree.Node
//                     // Leaves []types.ID
//                     // msg.
//                 }
//             }

//         }()

// 		wg.Wait()

//         srv.Debugf(">>> SUB COMPLETE %v/%v:", req.ChURI, string(req.NodePathname))

//     }

// 	//     DepthFirstIterator

// 	// // If only doing layers, don't search through EVERY node and be dum, step through the layer list and step through each layer, son!
// 	// if (req.FilterFlags & FilterFlags_LAYERS_ONLY) != 0 {
// 	// 	iter := state.ChildIterator(tree.Keypath(layersFork), false, 0)
// 	// 	defer iter.Close()

// 	// 	nodeBasePath = append(nodeBasePath[:0], tree.Keypath(nodesFork)...)

// 	// 	for iter.Rewind(); iter.Valid(); iter.Next() {
// 	// 		// Extract "<ParentID>/<NodeID>"
// 	// 		subPath := iter.Node().Keypath().LastNParts(2)
// 	// 		if subPath == nil {
// 	// 			continue
// 	// 		}
// 	// 		nodePath := append(nodeBasePath[:], subPath...)
// 	// 		nodePB := srv.filterAndExportNode(state.NodeAt(nodePath, nil), &exportFilters)
// 	// 		if nodePB == nil {
// 	// 			continue
// 	// 		}

// 	// 		err = server.Send(nodePB)
// 	// 		if err != nil {
// 	// 			return err
// 	// 		}
// 	// 	}

// 	// } else {
// 	// 	iter := state.ChildIterator(tree.Keypath(nodesFork), true, 20)
// 	// 	defer iter.Close()

// 	// 	for iter.Rewind(); iter.Valid(); iter.Next() {
// 	// 		nodePB := srv.filterAndExportNode(iter.Node(), &exportFilters)
// 	// 		if nodePB == nil {
// 	// 			continue
// 	// 		}

// 	// 		err = server.Send(nodePB)
// 	// 		if err != nil {
// 	// 			return err
// 	// 		}
// 	// 	}
// 	// }

// 	// if req.Subscribe {
// 	//     idx := 0
// 	//     for i := 0; i < 20; i++ {
// 	//         N := 200
// 	//         srv.Infof(0, "Sending #%v", N);

// 	//         for j := 0; j < N; j++ {
// 	//             idx++
// 	//             server.Send( &Node{
// 	//                 ID: "dummyID_" + strconv.Itoa(idx),
// 	//                 Name: "He bought?  DUMP IT.",
// 	//                 X1: mrand.NormFloat64() * 1000,
// 	//                 X2: mrand.NormFloat64() * 1000,
// 	//             })
// 	//         }
// 	//     }
// 	// }

// 	return nil
// }

// Key values used to map plan.Node fields to redwood node fields
const (
	NodeModifiedKey   = "t"
	NodeTypeIDKey     = "w"
	NodeLabelKey      = "n"
	NodeValueStrKey   = "s"
	NodeValueIntKey   = "i"
	NodeX1Key         = "1"
	NodeX2Key         = "2"
	NodeX3Key         = "3"
	NodeAttachmentKey = "a"

	NodeChMsgKey     = byte('\x10')
	NodeChMsgPathKey = "/\x10"
)

var chMsgKey = tree.Keypath([]byte{NodeChMsgKey})

// var nodesStartKey = []byte{0x01, 0x01}

// // Key values used to map plan.Node layer-related fields to redwood node fields
// // const (
// // 	SpaceTypeKey      = "spaceType"
// // 	SpaceT0AsUTCKey   = "t0utc"
// // 	SpaceX1UnitType   = "x1UnitType"
// // 	SpaceX2UnitType   = "x2UnitType"
// // 	SpaceX3UnitType   = "x3UnitType"
// // )

type nodeFilters struct {
	regexKeypath *regexp.Regexp
	regexTypeID  *regexp.Regexp
}

// acquireMsg is equivalent to:
//
// msg := &ChMsg{}
//
// msg.Unmarshal(buf)
func (job *reqJob) newResponse(op ChMsgOp) *ChMsg {

	// TODO: use sync.pool
	// https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277
	msg := &ChMsg{}

	msg.Op = op
	msg.ReqID = job.req.ReqID

	return msg
}

func (job *reqJob) newEntry(op ChMsgOp, unmarshalFromBase64 string) (*ChMsg, error) {

	var err error
	job.scrap, err = bufs.SmartDecodeFromBase64([]byte(unmarshalFromBase64), job.scrap)
	if err != nil {
		return nil, err
	}

	// TODO: use sync.pool
	// https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277
	msg := &ChMsg{}

	err = msg.Unmarshal(job.scrap)
	if err != nil {
		return nil, err
	}

	msg.Op = op
	msg.ReqID = job.req.ReqID

	return msg, nil
}

// releaseMsg releases this ChMsg back over to the pool
func releaseMsg(msg *ChMsg) {

	// TODO: use sync.pool here
	// if len(msg.Attachment) > 0 {
	// 	msg.Attachment = msg.Attachment[:0]
	// }
}

// filterAndSendNode takes a node given to be a rw key-value serialization of a ch entry and sends it to the job session outlet
func (job *reqJob) filterAndSendNode(node tree.Node) bool {
    sentNode := false

    {
		buf64, exists, err := node.StringValue(nil) //chMsgKey)
		if err != nil {
			job.sess.Warnf("error fetching ChMsg from node: %v, %v", string(node.Keypath()), err)
		} else if !exists {
			// no-op
		} else {

			// TODO: use sync.pool
			// https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277
			var msg *ChMsg
			msg, err = job.newEntry(ChMsgOp_ChEntry, buf64)
			if err != nil {
                job.sess.Warnf("error unmarshalling node: %v", err)
                return false
			}

			kp, _ := node.Keypath().Pop()
			msg.Keypath = string(kp)

            job.sess.msgOutlet <- msg

            sentNode = true
		}
	}

    return sentNode
}

// 	// Internal nodes (storing ChMsg fields
// 	leafName := node.Keypath().Pop()
// 	if len(leafName) <= 1 {
// 		return nil
// 	}

// 	nLastModified, exists, _ := node.IntValue(tree.Keypath(NodeModifiedKey))
// 	if exists == false {
// 		return nil
// 	}

// 	srv.Debugf("filterAndExportNode: %v", nLastModified)

// 	// nSpaceType, isLayer, err := node.StringValue(tree.Keypath(SpaceTypeKey))
// 	// if isLayer && (filters.FilterFlags&FilterFlags_EXCLUDE_LAYERS) != 0 {
// 	// 	return nil
// 	// }
// 	// if isLayer == false && (filters.FilterFlags&FilterFlags_LAYERS_ONLY) != 0 {
// 	// 	return nil
// 	// }

// 	// subPath := iter.Node().Keypath().LastNParts(1)
// 	// if subPath == nil {
// 	//     continue
// 	// }
// 	// nodePath := append(nodeBasePath[:], subPath...)

// 	passesStringFilter := func(node tree.Node, r *regexp.Regexp, key string, requiredField bool) (string, bool) {
// 		value, exists, err := node.StringValue(tree.Keypath(key))
// 		if err != nil {
// 			srv.Warnf("error fetching node key '%v': %v", key, err)
// 			return "", false
// 		} else if !exists && requiredField {
// 			srv.Warnf("node has no '%v' key", key)
// 			return "", false
// 		} else if exists == true && r != nil && r.Match([]byte(value)) == false {
// 			return "", false
// 		}
// 		return value, true
// 	}

// 	if job.filters.regexTypeID != nil && filters.regexNodeID.Match(nNodeID) == false {
// 		return nil
// 	}

// 	nName, passes := passesStringFilter(node, filters.regexName, NodeNameKey, false)
// 	if !passes {
// 		return nil
// 	}
// 	nTags, passes := passesStringFilter(node, filters.regexTags, NodeTagsKey, false)
// 	if !passes {
// 		return nil
// 	}
// 	nTypeID, passes := passesStringFilter(node, job.filters.regexTypeID, NodeTypeIDKey, false)
// 	if !passes {
// 		return nil
// 	}
// 	nT, _, _ := node.FloatValue(tree.Keypath(NodeTKey))
// 	nTSpan, _, err := node.FloatValue(tree.Keypath(NodeTSpanKey))
// 	if filters.timeSearch {
// 		tmax, tmin := filters.req.TMax, filters.req.TMin
// 		passes = tmin >= nT && nT <= tmax
// 		if passes {
// 			nTend := nT + nTSpan
// 			passes = tmin >= nTend && nTend <= tmax
// 		}
// 		if passes == false {
// 			return nil
// 		}
// 	}

// 	outNode := &Node{
// 		ID:           string(nNodeID),
// 		ParentPath:   string(nParentPath),
// 		Tags:         nTags,
// 		Name:         nName,
// 		TypeID:       nTypeID,
// 		LastModified: nLastModified,
// 		T:            nT,
// 		TSpan:        nTSpan,
// 	}

// 	outNode.Value, _, _ = node.StringValue(tree.Keypath(NodeValueKey))
// 	outNode.Glyph, _, _ = node.StringValue(tree.Keypath(NodeGlyphKey))

// 	outNode.X1, _, err = node.FloatValue(tree.Keypath(NodeX1Key))
// 	if err != nil {
// 		srv.Warnf("error fetching 'x1' from node: %v", err)
// 		return nil
// 	}
// 	outNode.X2, _, err = node.FloatValue(tree.Keypath(NodeX2Key))
// 	if err != nil {
// 		srv.Warnf("error fetching 'x2' from node: %v", err)
// 		return nil
// 	}
// 	outNode.X3, _, err = node.FloatValue(tree.Keypath(NodeX3Key))
// 	if err != nil {
// 		srv.Warnf("error fetching 'x3' from node: %v", err)
// 		return nil
// 	}

// 	{
// 		if val, exists, _ := node.Value(tree.Keypath(NodeTransform), nil); exists {
// 			array := val.([]interface{})
// 			if len(array) > 0 {
// 				outNode.Transform = make([]float32, len(array))
// 				for i, v := range array {
// 					outNode.Transform[i] = v.(float32)
// 				}
// 			}
// 		}
// 	}

// if (filters.NodeFetchFlags & FetchFlags_LINKS) != 0 {
//     if val, exists, _ := node.Value(tree.Keypath(NodeURIsKey), nil); exists {
//         array := val.([]interface{})
//         if len(array) > 0 {
//             outNode.Links = make([]string, len(array))
//             for i, v := range array {
//                 outNode.Links[i] = v.(string)
//             }
//         }
//     }
// }

// if (filters.NodeFetchFlags & FetchFlags_LINKS_RESOLVED) != 0 {
// 	linkexportFilters := exportFilters{
// 		NodeFetchFlags: filters.LinksFetchFlags,
// 	}

// 	for _, nodeURI := range outNode.Links {
// 		func() {
// 			linkType, linkValue := nelson.DetermineLinkType(nodeURI)

// 			if linkType == nelson.LinkTypeState {
// 				stateURI, keypath, version, err := nelson.ParseStateLink(linkValue)
// 				if err != nil {
// 					srv.Warnf("bad link URI: '%v'", nodeURI)
// 					return
// 				}

// 				childNodeState, err := srv.host.Controllers().StateAtVersion(stateURI, version)
// 				if err != nil {
// 					srv.Warnf("can't fetch node from link URI: '%v': %v", nodeURI, err)
// 					return
// 				}
// 				defer childNodeState.Close()

// 				childNode := childNodeState.NodeAt(keypath, nil)
// 				childNodePB := srv.filterAndExportNode(childNode, &linkexportFilters)
// 				if childNodePB == nil {
// 					return
// 				}

// 				outNode.LinksResolved = append(outNode.LinksResolved, childNodePB)
// 			}
// 		}()
// 	}
// }

// if (filters.req.LoadFields & NodeFields_BLOCK) != 0 {
// 	val, exists, err := node.BytesValue(tree.Keypath(BlockRawKey))
// 	if err != nil {
// 		srv.Warnf("error fetching '%v' subelement from node: %v", BlockRawKey, err)
// 	} else if !exists {
// 		// no-op
// 	} else {
// 		outNode.BlockRaw = val
// 	}
// }

// if len(outNode.BlockRaw) > 0 && (filters.NodeFetchFlags&FetchFlags_BLOCK_DECODED) != 0 {
// 	outNode.Block = &Block{}
// 	err := proto.Unmarshal(outNode.BlockRaw, outNode.Block)
// 	if err != nil {
// 		srv.Warnf("error unmarshalling Block: %v", err)
// 	}
// }

// if len(nSpaceType) > 0 {
// 	outNode.Layer = &NodeLayer{
// 		SpaceType: nSpaceType,
// 	}
// 	outNode.Layer.X1UnitType, _, _ = node.StringValue(tree.Keypath(SpaceX1UnitType))
// 	outNode.Layer.X2UnitType, _, _ = node.StringValue(tree.Keypath(SpaceX2UnitType))
// 	outNode.Layer.X3UnitType, _, _ = node.StringValue(tree.Keypath(SpaceX3UnitType))
// 	outNode.Layer.T0AsUTC, _, _ = node.FloatValue(tree.Keypath(SpaceT0AsUTCKey))
// }

// 	return outNode
// }

// func constructValMapForEntry(entry *ChMsg, vmap strMap) {

// 	// if numLinks := len(node.Links); numLinks > 0 {
// 	//     // nodeURIs := make([]interface{}, 0, numLinks)
// 	// 	// for _, nodeURI := range node.Links {
// 	//     //     nodeURIs = append(nodeURIs, strMap{
// 	// 	// 		"Content-Type": "link",
// 	// 	// 		"value":        nodeURI,
// 	// 	// 	})
// 	//     // }
// 	//     vmap[NodeURIsKey] = append([]interface{}{}, node.Links[:])
// 	// }

// 	if entry.Attachment != nil {
// 		vmap[NodeAttachmentKey] = entry.Attachment
// 	}
// 	if entry.TypeID != "" {
// 		vmap[NodeTypeIDKey] = entry.TypeID
// 	}
// 	if entry.Label != "" {
// 		vmap[NodeLabelKey] = entry.Label
// 	}
// 	if entry.ValueStr != "" {
// 		vmap[NodeValueStrKey] = entry.ValueStr
// 	}
// 	if entry.ValueInt != 0 {
// 		vmap[NodeValueIntKey] = entry.ValueInt
// 	}
// 	if entry.X3 != 0 {
// 		vmap[NodeX1Key] = entry.X1
// 		vmap[NodeX2Key] = entry.X2
// 		vmap[NodeX3Key] = entry.X3
// 	} else if entry.X2 != 0 {
// 		vmap[NodeX1Key] = entry.X1
// 		vmap[NodeX2Key] = entry.X2
// 	} else if entry.X1 != 0 {
// 		vmap[NodeX1Key] = entry.X1
// 	}

// 	// if entry.T != 0 {
// 	// 	vmap[NodeTKey] = entry.T
// 	// }
// 	// if entry.TSpan != 0 {
// 	// 	vmap[NodeTSpanKey] = entry.TSpan
// 	// }

// 	// if len(entry.Transform) > 0 {
// 	// 	vmap[NodeTransform] = append([]interface{}{}, entry.Transform[:])
// 	// }

// 	// // Encode layer/space info
// 	// if layer := node.Layer; layer != nil && layer.SpaceType != "" {
// 	// 	vmap[SpaceTypeKey] = layer.SpaceType
// 	// 	vmap[SpaceT0AsUTCKey] = layer.T0AsUTC
// 	// 	vmap[SpaceX1UnitType] = layer.X1UnitType
// 	// 	vmap[SpaceX2UnitType] = layer.X2UnitType
// 	// 	vmap[SpaceX3UnitType] = layer.X3UnitType
// 	// }

// }

// // Keypath is a POSIX-style pathname of this ChEntry (for safety, "/" specifies the root path, not "").
//             string              Keypath                     = 1;

// // If RecvChEntry, then this is a serialized ChEntry.
//             bytes               MsgData                     = 5;
// func (srv *GrpcServer) NodePut(ctx context.Context, req *NodePutReq) (*TxInfo, error) {

// 	tx := rw.Tx{
// 		StateURI: req.ChURI,
// 		ID:       rwtypes.RandomID(),
// 		Patches:  make([]rw.Patch, 0, 1+len(req.Nodes)),
// 	}

// 	err := srv.AddNodesAsPatches(req.Nodes, &tx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = srv.AddNodesAsPatches([]*Node{req.Node}, &tx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return srv.SendTx(ctx, &tx)

// }

// UnaryServerInterceptor is a debugging helper
func (srv *GrpcServer) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		srv.Debugf(trimStr(fmt.Sprintf("[rpc server] %v %+v", info.FullMethod, req), 500))

		x, err := handler(ctx, req)
		if err != nil {
			srv.Errorf(trimStr(fmt.Sprintf("[rpc server] %v %+v %+v", info.FullMethod, req, err), 500))
		}

		srv.Debugf(trimStr(fmt.Sprintf("[rpc server] %v %+v, %+v", info.FullMethod, req, x), 500))
		return x, err
	}
}

// StreamServerInterceptor is a debugging helper
func (srv *GrpcServer) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(server interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		srv.Infof(2, "[rpc server] %v", info.FullMethod)
		err := handler(server, stream)
		if err != nil {
			srv.Errorf("[rpc server] %+v", err)
		}
		return err
	}
}

func trimStr(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
