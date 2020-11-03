package repo

import (
	"context"
	"fmt"
	"net"
	"path"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	//proto "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	grpc_codes "google.golang.org/grpc/codes"

	"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/ctx"
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

// func formNodePath(parentPath string, nodeID string) tree.Keypath {
// 	k := make(tree.Keypath, 0, 192)
// 	k = append(k, tree.Keypath(parentPath)...)
// 	k = append(k, tree.Keypath(nodeID)...)
// 	return k
// }

// GrpcServer is the GRPC implementation of repo.proto
type GrpcServer struct {
	ctx.Context

	sessCount int32
	//repoSess      []*repoSess
	host          Host
	server        *grpc.Server
	listenNetwork string
	listenAddr    string
}

// NewGrpcServer creates a new GrpcServer
func NewGrpcServer(host Host, listenNetwork string, listenAddr string) *GrpcServer {
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
			srv.Info(1, "initiating graceful stop")

			// This closes the net.Listener as well.
			go srv.server.GracefulStop()

			// Give the server a chance to send out cancels for all open jobs and existing jobs to finish.
			go func() {
				time.Sleep(500 * time.Millisecond)
				srv.server.Stop()
			}()

		},
		nil,
		func() {
			srv.server.Stop()
			srv.Info(1, "grpc stop complete")
		},
	)
}

type strMap = map[string]interface{}

type reqJob struct {
	req      *ChReq
	sess     *repoSess
	filters  nodeFilters
	scrap    []byte
	canceled bool
	chSub    ChSub
	// ctx       context.Context
	// ctxCancel context.CancelFunc
}

// Debugf prints output to the output log
func (job *reqJob) Debugf(msgFormat string, msgArgs ...interface{}) {
	job.sess.Infof(2, msgFormat, msgArgs...)
}

// canceled returns true if this job should back out of all work.
func (job *reqJob) isCanceled() bool {
	return job.canceled
}

func (job *reqJob) cancelJob() {
	job.canceled = true
	if job.chSub != nil {
		job.chSub.Close()
	}
}

func (job *reqJob) exeGetOp() error {
	var err error
	job.chSub, err = job.sess.srv.host.OpenChSub(job.req)
	if err != nil {
		return err
	}
	defer job.chSub.Close()

	// Block while the chSess works and outputs ch entries to send from the ch session.
	// If/when the chSess see the job ctx stopping, it will unwind and close the outbox
	{
		for node := range job.chSub.Outbox() {
			job.sess.nodeOutbox <- node
		}
	}

	return nil
}

func (job *reqJob) exeTxOp() (*Node, error) {

	if job.req.TxOp.ChStateURI == nil {
		job.req.TxOp.ChStateURI = job.req.ChStateURI
	}

	tx, err := job.sess.hostSess.EncodeToTxAndSign(job.req.TxOp)
	if err != nil {
		return nil, err
	}

	// TODO: don't release this op until its merged or rejected (required tx broadcast)
	err = job.sess.srv.host.SubmitTx(tx)
	if err != nil {
		return nil, err
	}

	node := job.newResponse(NodeOp_ReqComplete)
	node.Attachment = append(node.Attachment[:0], tx.TID...)
	node.Str = path.Join(job.req.ChStateURI.DomainName, TID(tx.TID).Base32())

	return node, nil
}

func (job *reqJob) exeJob() {
	var err error
	var node *Node

	// Check to see if this req is canceled before beginning
	if err == nil {
		if job.isCanceled() {
			err = ErrCode_ReqCanceled.Err()
		}
	}

	if err == nil {
		if job.req.ChStateURI == nil && len(job.req.ChURI) > 0 {
			job.req.ChStateURI = &ChStateURI{}
			err = job.req.ChStateURI.AssignFromURI(job.req.ChURI)
		}
	}

	if err == nil {
		switch job.req.ReqOp {

		case ChReqOp_Auto:
			switch {
			case job.req.GetOp != nil:
				err = job.exeGetOp()
			case job.req.TxOp != nil:
				node, err = job.exeTxOp()
			}

		default:
			err = ErrCode_UnsupporteReqOp.Err()
		}
	}

	// Send completion msg
	{
		if err == nil && job.isCanceled() {
			err = ErrCode_ReqCanceled.Err()
		}

		if err != nil {
			node = job.req.newResponse(NodeOp_ReqDiscarded, err)
		} else if node == nil {
			node = job.newResponse(NodeOp_ReqComplete)
		} else if node.Op != NodeOp_ReqComplete && node.Op != NodeOp_ReqDiscarded {
			panic("this should be msg completion")
		}

		job.sess.nodeOutbox <- node
	}

	job.sess.removeJob(job.req.ReqID)
}

type repoSess struct {
	ctx.Context

	srv        *GrpcServer
	openReqs   map[int32]*reqJob
	openReqsMu sync.Mutex
	nodeOutbox chan *Node
	hostSess   HostSession
	scrap      [512]byte
	rpc        RepoGrpc_RepoServiceSessionServer
}

func (sess *repoSess) ctxStartup() error {

	// Send outgoing msgs
	sess.CtxGo(func() {
		for running := true; running; {
			select {
			case msg := <-sess.nodeOutbox:
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
			<-sess.nodeOutbox
		}
	})

	// Process incoming requests
	sess.CtxGo(func() {
		for sess.CtxRunning() {
			reqIn, err := sess.rpc.Recv()
			if err != nil {
				if grpc.Code(err) == grpc_codes.Canceled {
					sess.CtxStop("session pipe canceled", nil)
				} else {
					sess.Errorf("sess.rpc.Recv() err: %v", err)
				}
			}
			sess.dispatchReq(reqIn)
		}

		sess.Info(2, "sess rpc reader exited")
	})

	return nil
}

func (sess *repoSess) ctxStopping() {
	sess.cancelAll()
}

func (sess *repoSess) lookupJob(reqID int32) *reqJob {
	sess.openReqsMu.Lock()
	job := sess.openReqs[reqID]
	sess.openReqsMu.Unlock()
	return job
}

func (sess *repoSess) removeJob(reqID int32) {
	sess.openReqsMu.Lock()
	{
		delete(sess.openReqs, reqID)

		// Send an empty msg to wake up pipe shutdown
		sess.nodeOutbox <- nil
	}
	sess.openReqsMu.Unlock()
}

func (sess *repoSess) numJobsOpen() int {
	sess.openReqsMu.Lock()
	N := len(sess.openReqs)
	sess.openReqsMu.Unlock()
	return N
}

func (sess *repoSess) addNewJob(reqIn *ChReq) *reqJob {
	job := &reqJob{
		req:  reqIn,
		sess: sess,
	}

	//job.ctx, job.ctxCancel = context.WithCancel(sess.Ctx())

	sess.openReqsMu.Lock()
	sess.openReqs[reqIn.ReqID] = job
	sess.openReqsMu.Unlock()

	return job
}

func (sess *repoSess) dispatchReq(reqIn *ChReq) {
	if reqIn == nil {
		return
	}

	var err error

	job := sess.lookupJob(reqIn.ReqID)
	if reqIn.ReqOp == ChReqOp_CancelReq {
		if job != nil {
			job.cancelJob()
		} else {
			err = ErrCode_ReqIDNotFound.Err()
		}
	} else {
		if job != nil {
			sess.Warnf("client sent ReqID already in use (ReqID=%v)", reqIn.ReqID)
		} else {
			job := sess.addNewJob(reqIn)
			go job.exeJob()
		}
	}

	// Sends an error if reqErr.Code was set
	if err != nil {
		sess.nodeOutbox <- reqIn.newResponse(NodeOp_ReqDiscarded, err)
	}
}

func (sess *repoSess) cancelAll() {
	sess.Info(2, "canceling all jobs")
	jobsCanceled := 0
	sess.openReqsMu.Lock()
	for _, job := range sess.openReqs {
		if job.isCanceled() == false {
			jobsCanceled++
			job.cancelJob()
		}
	}
	sess.openReqsMu.Unlock()
	if jobsCanceled > 0 {
		sess.Infof(1, "canceled %v jobs", jobsCanceled)
	}
}

// RepoServiceSession is the Grpc session a client opens and keeps open.
// Multiple pipes can be open at any time by the same client or multiple clients.
func (srv *GrpcServer) RepoServiceSession(rpc RepoGrpc_RepoServiceSessionServer) error {

	sess := &repoSess{
		srv:        srv,
		openReqs:   make(map[int32]*reqJob),
		nodeOutbox: make(chan *Node, 4),
		hostSess:   srv.host.NewSession(),
		rpc:        rpc,
	}

	sess.SetLogLabelf("repoSess%2d", atomic.AddInt32(&srv.sessCount, 1))

	err := sess.CtxStart(
		sess.ctxStartup,
		nil,
		nil,
		sess.ctxStopping,
	)
	if err != nil {
		return err
	}

	select {
	// case <-srv.Ctx().Done():
	// 	sess.Info(2, "stopping")
	// 	sess.CtxStop(srv.CtxStopReason(), nil)
	case <-rpc.Context().Done():
		sess.Info(2, "rpc.Context().Done()")
	}

	sess.CtxWait()

	return nil
}

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

var (
	nodeEntryKey = byte('\x10')
	//nodeEntryPath = tree.Keypath([]byte{tree.PathSepChar, nodeEntryKey})
)

type nodeFilters struct {
	regexKeypath *regexp.Regexp
	regexTypeID  *regexp.Regexp
}

func (req *ChReq) newResponseFromCopy(src *Node) *Node {

	// TODO: use sync.pool
    // https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277

    node := *src
	node.ReqID = req.ReqID

	return &node
}

func (req *ChReq) newResponse(op NodeOp, err error) *Node {

	// TODO: use sync.pool
	// https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277
	node := &Node{}

	node.Op = op
	node.ReqID = req.ReqID

	if err != nil {
		var reqErr *ReqErr
		if reqErr, _ = err.(*ReqErr); reqErr == nil {
			err = ErrCode_UnnamedErr.Wrap(err)
			reqErr = err.(*ReqErr)
		}
		node.Attachment = bufs.SmartMarshal(reqErr, node.Attachment)
	}

	return node
}

func (job *reqJob) newResponse(op NodeOp) *Node {
	return job.req.newResponse(op, nil)
}

func (req *ChReq) newChEntry(NodeBuf []byte) (*Node, error) {

	// TODO: use sync.pool
	// https://medium.com/a-journey-with-go/go-understand-the-design-of-sync-pool-2dde3024e277
	node := &Node{}

	err := node.Unmarshal(NodeBuf)
	if err != nil {
		return nil, err
	}

	node.ReqID = req.ReqID

	return node, nil
}

// releaseMsg releases this Node back over to the pool
func releaseMsg(node *Node) {

	// TODO: use sync.pool here
	// if len(node.Attachment) > 0 {
	// 	node.Attachment = node.Attachment[:0]
	// }
}

// 	// Internal nodes (storing Node fields
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
