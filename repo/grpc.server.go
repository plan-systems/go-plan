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

// GrpcServer is the GRPC implementation of repo.proto
type GrpcServer struct {
	ctx.Context

	sessCount     int32
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

type reqJob struct {
	req      *ChReq
	sess     *repoSess
	filters  nodeFilters
	scrap    []byte
	canceled bool
	chSub    ChSub
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

	tx, err := job.sess.membSess.EncodeToTxAndSign(job.req.TxOp)
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
	membSess   MemberSession
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
	delete(sess.openReqs, reqID)
	sess.openReqsMu.Unlock()

	// Send an empty msg to wake up and check for shutdown
	sess.nodeOutbox <- nil
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
		membSess:   srv.host.NewSession(),
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
	node := &Node{
		Op:    op,
		ReqID: req.ReqID,
	}

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
