// Package ctx provides project-agnostic utility code for Go projects
package ctx

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Ctx is an abstraction for a context that can stopped/aborted.
//
// Ctx serves as a vehicle between a Content expressed as *struct or as an interface (which can be upcast).
type Ctx interface {
	BaseContext() *Context
}

type childCtx struct {
	CtxID   []byte
	Ctx     Ctx
	Context *Context
}

// Context is a service helper.
type Context struct {
	Logger

	Ctx        context.Context
	FaultLog   []error
	FaultLimit int

	// internal
	ctxCancel     context.CancelFunc
	stopComplete  sync.WaitGroup
	stopMutex     sync.Mutex
	stopReason    string
	parent        *Context
	children      []childCtx
	childrenMutex sync.RWMutex

	// externally provided callbacks
	onAboutToStop      func()
	onChildAboutToStop func(inChild Ctx)
	onStopping         func()
}

// CtxStopping allows callers to wait until this context is stopping.
func (C *Context) CtxStopping() <-chan struct{} {
	return C.Ctx.Done()
}

// CtxStart blocks until the given onStartup proc is complete  started up or stopped (if an err is returned).
//
// See notes for CtxInitiateStop()
func (C *Context) CtxStart(
	onStartup func() error,
	onAboutToStop func(),
	onChildAboutToStop func(inChild Ctx),
	onStopping func(),
) error {

	if C.CtxRunning() {
		panic("plan.Context already running")
	}

	C.onAboutToStop = onAboutToStop
	C.onChildAboutToStop = onChildAboutToStop
	C.FaultLimit = 1
	C.Ctx, C.ctxCancel = context.WithCancel(context.Background())

	var err error
	if onStartup != nil {
		err = onStartup()
	}

	// Even if onStopping == nil, we still need this call since the resulting C.stopComplete.Add(1) ensures
	// that this context's CtxStop() has actually somethong to wait on.
	Go(C, func(inCtx Ctx) {

		// This whole onStopping is a convenience as well as a self-documenting thing,  When looking at code,
		// some can make sense of a function passed as an "onStopping" proc more easily than some generic call that
		// is less discernable.
		select {
			case <-inCtx.BaseContext().CtxStopping():
		}

		if onStopping != nil {
			onStopping()
		}
	})

	if err != nil {
		C.Errorf("CtxStart failed: %v", err)
		C.CtxStop("CtxStart failed", nil)
		C.CtxWait()
	}

	return err
}

// CtxStop initiates a stop of this Context, calling inReleaseOnComplete.Done() when this context is fully stopped (if provided).
//
// If C.CtxStop() is being called for the first time, true is returned.
// If it has already been called (or is in-flight), then false is returned and this call effectively is a no-op (but still honors in inReleaseOnComplete).
//
//	The following are equivalent:
//
//			initiated := c.CtxStop(reason, waitGroup)
//
//			initiated := c.CtxStop(reason, nil)
//			c.CtxWait()
//			if waitGroup != nil {
//				waitGroup.Done()
//			}
//
// When C.CtxStop() is called (for the first time):
//  1. C.onAboutToStop() is called (if provided to C.CtxStart)
//  2. if C has a parent, C is detached and the parent's onChildAboutToStop(C) is called (if provided to C.CtxStart).
//  3. C.CtxStopChildren() is called, blocking until all children are stopped (recursive)
//  4. C.Ctx is cancelled, causing onStopping() to be called (if provided) and any <-CtxStopping() calls to be unblocked.
//  5. C's onStopping() is called (if provided to C.CtxStart)
//  6. After the last call to C.CtxGo() has completed, C.CtxWait() is released.
//
func (C *Context) CtxStop(
	inReason string,
	inReleaseOnComplete *sync.WaitGroup,
) bool {

	initiated := false

	C.stopMutex.Lock()
	if ctxCancel := C.ctxCancel; C.CtxRunning() && ctxCancel != nil {
		C.ctxCancel = nil
		C.stopReason = inReason
		C.Infof(2, "CtxStop (%s)", C.stopReason)

		// 3. Now hand it over to client-level execution to finish stopping/cleanup.
		if onAboutToStop := C.onAboutToStop; onAboutToStop != nil {
			C.onAboutToStop = nil
			onAboutToStop()
		}

		if C.parent != nil {
			C.parent.childStopping(C)
		}

		// 1_ Stop the all children so that leaf children are stopped first.
		C.CtxStopChildren("parent is stopping")

		// 2. Calling this *after* stopping children the entire hierarchy to closed leaf-first.
		ctxCancel()

		initiated = true
	}
	C.stopMutex.Unlock()

	if inReleaseOnComplete != nil {
		C.CtxWait()
		inReleaseOnComplete.Done()
	}

	return initiated
}

// CtxWait blocks until this Context has completed stopping (following a CtxInitiateStop). Returns true if this call initiated the shutdown (vs another cause)
//
// THREADSAFE
func (C *Context) CtxWait() {
	C.stopComplete.Wait()
}

// CtxStopChildren initiates a stop on each child and blocks until complete.
func (C *Context) CtxStopChildren(inReason string) {

	childrenRunning := &sync.WaitGroup{}

	logInfo := C.LogV(2)

	C.childrenMutex.RLock()
	N := len(C.children)
	if logInfo {
		C.Infof(2, "%d children to stop", N)
	}
	if N > 0 {
		childrenRunning.Add(N)
		for i := N - 1; i >= 0; i-- {
			child := C.children[i].Ctx
			childC := child.BaseContext()
			if logInfo {
				C.Infof(2, "stopping child <%s> (%s)", childC.GetLogLabel(), reflect.TypeOf(child).Elem().Name())
			}
			go childC.CtxStop(inReason, childrenRunning)
		}
	}
	C.childrenMutex.RUnlock()

	childrenRunning.Wait()
	if N > 0 && logInfo {
		C.Infof(2, "children stopped")
	}
}

// CtxGo is lesser more convenient variant of Go().
func (C *Context) CtxGo(
	inProcess func(),
) {
	C.stopComplete.Add(1)
	go func(C *Context) {
		inProcess()
		C.stopComplete.Done()
	}(C)
}

// CtxAddChild adds the given Context to this Context as a "child", meaning that
// the parent context will wait for all children's CtxStop() to complete before
func (C *Context) CtxAddChild(
	inChild Ctx,
	inID []byte,
) {

	C.childrenMutex.Lock()
	C.children = append(C.children, childCtx{
		CtxID: inID,
		Ctx:   inChild,
	})
	inChild.BaseContext().setParent(C)
	C.childrenMutex.Unlock()
}

// CtxGetChildByID returns the child Context with the match ID (or nil if not found).
func (C *Context) CtxGetChildByID(
	inChildID []byte,
) Ctx {
	var ctx Ctx

	C.childrenMutex.RLock()
	N := len(C.children)
	for i := 0; i < N; i++ {
		if bytes.Equal(C.children[i].CtxID, inChildID) {
			ctx = C.children[i].Ctx
		}
	}
	C.childrenMutex.RUnlock()

	return ctx
}

// CtxStopReason returns the reason provided by the stop initiator.
func (C *Context) CtxStopReason() string {
	return C.stopReason
}

// CtxStatus returns an error if this Context has not yet started, is stopping, or has stopped.
//
// THREADSAFE
func (C *Context) CtxStatus() error {

	if C.Ctx == nil {
		return ErrCtxNotRunning
	}

	return C.Ctx.Err()
}

// CtxRunning returns true has been started and is no stopping.
//
// Warning: since this go from true to false at any time, this call is typically
// used to poll a context in order to detect if/when a workflow should cease.
//
// THREADSAFE
func (C *Context) CtxRunning() bool {

	if C.Ctx == nil {
		return false
	}

	select {
	case <-C.Ctx.Done():
		return false
	default:
	}

	return true
}

// CtxOnFault is called when the given error has occurred an represents an unexpected fault that alone doesn't
// justify an emergency condition. (e.g. a db error while accessing a record).  Call this on unexpected errors.
//
// If inErr == nil, this call has no effect.
//
// THREADSAFE
func (C *Context) CtxOnFault(inErr error, inDesc string) {

	if inErr == nil {
		return
	}

	C.Error(inDesc, ": ", inErr)

	C.stopMutex.Lock()
	C.FaultLog = append(C.FaultLog, inErr)
	faultCount := len(C.FaultLog)
	C.stopMutex.Unlock()

	if faultCount < C.FaultLimit {
		return
	}

	C.CtxStop("fault limit reached", nil)
}

// setParent is internally called wheen attaching/detaching a child.
func (C *Context) setParent(inNewParent *Context) {
	if inNewParent != nil && C.parent != nil {
		panic("Context already has parent")
	}
	C.parent = inNewParent
}

// BaseContext allows the holder of a Ctx to get the raw/underlying Context.
func (C *Context) BaseContext() *Context {
	return C
}

// childStopping is internally called once the given child has been stopped and its C.onAboutToStop() has completed.
func (C *Context) childStopping(
	inChild *Context,
) {

	var native Ctx

	// Detach the child
	C.childrenMutex.Lock()
	inChild.setParent(nil)
	N := len(C.children)
	for i := 0; i < N; i++ {

		// A downside of Go is that a struct that embeds ptools.Context can show up as two interfaces:
		//    Ctx(&item.Context) and Ctx(&item)
		// Since all the callbacks need to be the latter "native" Ctx (so that it can be upcast to a client type),
		//    we must ensure that we search for ptr matches using the "base" Context but callback with the native.
		native = C.children[i].Ctx
		if native.BaseContext() == inChild {
			copy(C.children[i:], C.children[i+1:N])
			N--
			C.children[N].Ctx = nil
			C.children = C.children[:N]
			break
		}
		native = nil
	}
	C.childrenMutex.Unlock()

	if C.onChildAboutToStop != nil {
		C.onChildAboutToStop(native)
	}
}

// AttachInterruptHandler creates a new interupt handler and fuses it w/ the given context
func (C *Context) AttachInterruptHandler() {
	sigInbox := make(chan os.Signal, 1)

	signal.Notify(sigInbox, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		count := 0
		firstTime := int64(0)

		//timer := time.NewTimer(30 * time.Second)

		for sig := range sigInbox {
			count++
			curTime := time.Now().Unix()

			// Prevent un-terminated ^C character in terminal
			fmt.Println()

			if count == 1 {
				firstTime = curTime

				reason := "received " + sig.String()
				C.CtxStop(reason, nil)
			} else {
				if curTime > firstTime+3 {
					fmt.Println("\nReceived interrupt before graceful shutdown, terminating...")
					os.Exit(-1)
				}
			}
		}
	}()

	go func() {
		C.CtxWait()
		signal.Stop(sigInbox)
		close(sigInbox)
	}()

	C.Infof(0, "for graceful shutdown, \x1b[1m^C\x1b[0m or \x1b[1mkill -s SIGINT %d\x1b[0m", os.Getpid())
}

// AttachGrpcServer opens a new net.Listener for the given netwok params and then serves ioServer.
//
// Appropriate calls are set up so that:
//  1. ioServer exiting will trigger CtxStop().
//  2. When this Context is about to stop, ioServer.GracefulStop is called.
func (C *Context) AttachGrpcServer(
	inNetwork string,
	inNetAddr string,
	ioServer *grpc.Server,
) error {

	C.stopMutex.Lock()
	defer C.stopMutex.Unlock()

	C.Infof(0, "starting grpc service on \x1b[1;32m%v %v\x1b[0m", inNetwork, inNetAddr)
	listener, err := net.Listen(inNetwork, inNetAddr)
	if err != nil {
		return err
	}

	// Register reflection service on gRPC server.
	reflection.Register(ioServer)
	Go(C, func(inCtx Ctx) {
		C := inCtx.BaseContext()

		if err := ioServer.Serve(listener); err != nil {
			C.Error("grpc server error: ", err)
		}
		listener.Close()

		C.CtxStop("grpc server stopped", nil)
	})

	origAboutToStop := C.onAboutToStop
	C.onAboutToStop = func() {
		C.Info(1, "stopping grpc service")
		go ioServer.GracefulStop()

		if origAboutToStop != nil {
			origAboutToStop()
		}
	}

	return err
}

// Go starts inProcess() in its own go routine while preventing inHostCtx from stopping until inProcess has completed.
//
// 	In effect, CtxGo(inHostCtx, inProcess) replaces:
//
//		go inProcess(inHostCtx)
//
// The presumption here is that inProcess will exit from some trigger *other* than inHost.CtxWait()
func Go(
	inHost Ctx,
	inProcess func(inHost Ctx),
) {
	inHost.BaseContext().stopComplete.Add(1)
	go func(inHost Ctx) {
		inProcess(inHost)
		inHost.BaseContext().stopComplete.Done()
	}(inHost)
}
