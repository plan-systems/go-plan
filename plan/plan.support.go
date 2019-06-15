package plan

import (
	"bytes"
	"context"
	"encoding/hex"
    "encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
    "reflect"
	"strings"
	"sync"
	"time"

	crand "crypto/rand"

	"github.com/plan-systems/klog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func init() {

	klog.InitFlags(nil)
	klog.SetFormatter(&klog.FmtConstWidth{
		FileNameCharWidth: 20,
		UseColor:          true,
	})

}

// Blob is a convenience function that forms a ChID byte array from a ChID byte slice.
func (chID ChID) Blob() ChIDBlob {

	var blob ChIDBlob
	copy(blob[:], chID)

	return blob
}

// Str returns this channel ID in plan.Base64p form.
func (chID ChID) Str() string {
	return Base64p.EncodeToString(chID)
}

// SuffixStr returns the last few digits of this ChID in string form (for easy reading, logs, etc)
func (chID ChID) SuffixStr() string {
	return Base64p.EncodeToString(chID[ChIDSz-6:])
}

// AssignFromTID copies from the right-part of the given TID
func (chID ChID) AssignFromTID(tid TID) {
	copy(chID, tid[TIDSz-ChIDSz:])
}

// TID is a convenience function that returns the TID contained within this TIDBlob.
func (tid *TIDBlob) TID() TID {
	return tid[:]
}

// IsNil returns true if this TID length is 0 or is equal to NilTID
func (tid TID) IsNil() bool {
	if len(tid) != TIDSz {
		return false
	}

	if bytes.Equal(tid, NilTID[:]) {
		return true
	}

	return false
}

// Blob is a convenience function that forms a TID byte array from a TID byte slice.
func (tid TID) Blob() TIDBlob {

	var blob TIDBlob
	copy(blob[:], tid)

	return blob
}

// Str returns this TID in plan.Base64 form.
func (tid TID) Str() string {
	return Base64p.EncodeToString(tid)
}

// SuffixStr returns the last few digits of this TID in string form (for easy reading, logs, etc)
func (tid TID) SuffixStr() string {
	return Base64p.EncodeToString(tid[TIDSz-6:])
}

// SetTimeAndHash writes the given timestamp and the right-most part of inSig into this TID.
//
// Byte layout is designed so that TIDs can be sorted by its leading embedded timestamp:
//    0:6   - Standard UTC timestamp in unix seconds (BE)
//    6:8   - Timestamp fraction (BE)
//    8:27  - Hash bytes
func (tid TID) SetTimeAndHash(inTime TimeFS, inHash []byte) {

	tid.SetTimeFS(inTime)
	tid.SetHash(inHash)
}

// SetHash set the sig/hash portion of this ID
func (tid TID) SetHash(inHash []byte) {

	const TIDHashSz = TIDSz - 8
	pos := len(inHash) - TIDHashSz
	if pos >= 0 {
		copy(tid[8:], inHash[pos:])
	} else {
		for i := 8; i < TIDSz; i++ {
			tid[i] = 0
		}
	}
}

// SetTimeFS writes the given timestamp into this TIS
func (tid TID) SetTimeFS(inTime TimeFS) {

	tid[0] = byte(inTime >> 56)
	tid[1] = byte(inTime >> 48)
	tid[2] = byte(inTime >> 40)
	tid[3] = byte(inTime >> 32)
	tid[4] = byte(inTime >> 24)
	tid[5] = byte(inTime >> 16)
	tid[6] = byte(inTime >> 8)
	tid[7] = byte(inTime)

}

// ExtractTimeFS returns the unix timestamp embedded in this TID (a unix timestamp in 1<<16 seconds UTC)
func (tid TID) ExtractTimeFS() TimeFS {

	t := int64(tid[0])
	t = (t << 8) | int64(tid[1])
	t = (t << 8) | int64(tid[2])
	t = (t << 8) | int64(tid[3])
	t = (t << 8) | int64(tid[4])
	t = (t << 8) | int64(tid[5])
	t = (t << 8) | int64(tid[6])
	t = (t << 8) | int64(tid[7])

	return TimeFS(t)
}

// ExtractTime returns the unix timestamp embedded in this TID (a unix timestamp in seconds UTC)
func (tid TID) ExtractTime() int64 {

	t := int64(tid[0])
	t = (t << 8) | int64(tid[1])
	t = (t << 8) | int64(tid[2])
	t = (t << 8) | int64(tid[3])
	t = (t << 8) | int64(tid[4])
	t = (t << 8) | int64(tid[5])

	return t
}

// SelectEarlier looks in inTime and if it is later than the time embedded in ioURID, then this function has no effect and returns false.
// If inTime is earlier than the embedded time, then ioURID is initialized to inTime (and zeroed out) and returns true.
func (tid TID) SelectEarlier(inTime TimeFS) bool {

	t := tid.ExtractTimeFS()

	// Timestamp value of 0 is reserved and should only reflect an invalid/uninitialized TID.
	if inTime < 0 {
		inTime = 0
	}

	if inTime < t || inTime == 0 {
		tid[0] = byte(inTime >> 56)
		tid[1] = byte(inTime >> 48)
		tid[2] = byte(inTime >> 40)
		tid[3] = byte(inTime >> 32)
		tid[4] = byte(inTime >> 24)
		tid[5] = byte(inTime >> 16)
		tid[6] = byte(inTime >> 8)
		tid[7] = byte(inTime)

		for i := 8; i < len(tid); i++ {
			tid[i] = 0
		}
		return true
	}

	return false
}

// CopyNext copies the given TID and increments it by 1, typically useful for seeking the next entry after a given one.
func (tid TID) CopyNext(inTID TID) {
	copy(tid, inTID)
	for j := len(tid) - 1; j > 0; j-- {
		tid[j]++
		if tid[j] > 0 {
			break
		}
	}
}

// SmartMarshal marshals the given item to the given buffer.  If there is not enough space
func SmartMarshal(inItem Marshaller, ioBuf []byte) []byte {
	var (
		err error
	)

	max := cap(ioBuf)
	sz := inItem.Size()
	if sz > max {
		ioBuf = make([]byte, sz)
		max = sz
	}

	sz, err = inItem.MarshalTo(ioBuf[:max])
	if err != nil {
		panic(err)
	}

	return ioBuf[:sz]
}

// Ctx is an abstraction for a context that can stopped/aborted.
type Ctx interface {

    CtxStop(inReason string)

    CtxIDStr() string

    setParent(inNewParent Ctx)
    getParent() Ctx
    detachChild(inChild Ctx)
}


type childCtx struct {
    CtxID string
    Ctx   Ctx
}

// Context is a service helper.
type Context struct {
	Logger

    
	Ctx          context.Context
	ctxCancel    context.CancelFunc

	stopComplete sync.WaitGroup
	stopMutex    sync.Mutex
	stopReason   string

    parent          Ctx
    children        []childCtx
    childrenMutex   sync.RWMutex
    childStopping   func(*Context)

    // Identifies this context from others
    CtxID           string

	FaultLog   []error
	FaultLimit int
}



// CtxStart blocks until this Context is started up or stopped (if an err is returned).
//
// See notes for CtxInitiateStop()
func (C *Context) CtxStart(
	parentCtx context.Context,
    onStartup func() error,
	onStopping func(),
    onChildStopping func(inChild *Context),
) error {

	C.stopComplete.Wait()
	C.stopComplete.Add(1)

	C.FaultLimit = 1
	C.Ctx, C.ctxCancel = context.WithCancel(parentCtx)

	err := onStartup()
	if err != nil {
		C.Errorf("CtxStart failed: %v", err)
		C.CtxInitiateStop("CtxStart failed")
	}

	go func() {

		select {
		    case <-C.Ctx.Done():
		}

		C.stopMutex.Lock()
        if C.ctxCancel != nil {
            // If we're here, it's because the parent context is done (not because CtxStop()/ctxCancel was called)
            C.ctxCancel = nil
            C.stopReason = C.Ctx.Err().Error()
        }
		C.stopMutex.Unlock()

		C.Info(2, "plan.Context done: ", C.Ctx.Err())
        
        if C.parent != nil {
            C.parent.detachChild(C)
        }

        C.CtxStopChildren(C.stopReason)

        onStopping()

		C.stopComplete.Done()
	}()

	if err != nil {
		C.stopComplete.Wait()
	}

	return err
}

// CtxInitiateStop initiates a polite shutdown of this Context, returning immediately after.
// If Shutdown() has already been called, behavior is consistent but inReason will be dropped.
//
// THREADSAFE
//
// When a C.CtxInitiateStop() is called
//  1. C is detached from its parent (if set)
//  2. C's parent's OnChildStopping callback is made (if set)
//  3. C's children are stopped (recursive)
//  4. C's OnStopping callback is made
//  5. C.StopComplete.Done() is called (which should release all Wait() calls against it)
func (C *Context) CtxInitiateStop(
	inReason string,
) bool {

	initiated := false

	C.stopMutex.Lock()
    ctxCancel := C.ctxCancel
	if C.CtxRunning() && ctxCancel != nil {
		C.ctxCancel = nil
		C.stopReason = inReason
		C.Infof(2, "initiating CtxStop (%s)", C.stopReason)
		ctxCancel()
		initiated = true
	}
	C.stopMutex.Unlock()

	return initiated
}

// CtxStop calls InitiateShutdown() and blocks until complete. Returns true if this call initiated the shutdown (vs another cause)
//
// THREADSAFE
//
func (C *Context) CtxStop(
	inReason string,
) {
	C.CtxInitiateStop(inReason)
	C.stopComplete.Wait()
    C.Infof(2, "completed CtxStop")
}

// CtxStopChildren initiates a stop on each child and blocks until complete.
func (C *Context) CtxStopChildren(inReason string) {

    // Shutdown the Stores FIRST so that all we have to do is wait on the server to stop.
    childCount := sync.WaitGroup{}

    C.childrenMutex.RLock()
    N := len(C.children)
    if N > 0 {
        C.Infof(2, "stopping %d children", N)
        childCount.Add(N)
        for i := N-1; i >= 0; i-- {
            child := C.children[i].Ctx
            go func() {
                child.CtxStop(inReason)
                childCount.Done()
            }()
        }
    }
    C.childrenMutex.RUnlock()

    childCount.Wait()
    C.Infof(2, "%d children stopped", N)

}

// CtxGo is equivalent to go f() except that this Context will wait until ctxProcess completes before it signals that it is stopped.  
// The presumption here is that ctxProcess will exit on its own or shortly after this Context is no longer running.
func (C *Context) CtxGo(ctxProcess func(inParent *Context)) {
    C.stopComplete.Add(1)
    go func(){
        ctxProcess(C)
        C.stopComplete.Done()
    }()
}

// CtxIDStr returns a string uniquely identifying this context.
func (C *Context) CtxIDStr() string {
    return C.CtxID
}

// CtxAddChild adds the given Context to this Context as a "child", meaning that 
// the parent context will wait for all children's CtxStop() to complete before 
func (C *Context) CtxAddChild(
    inChild Ctx,
) {
    childID := inChild.CtxIDStr()

    C.childrenMutex.Lock()
    C.children = append(C.children, childCtx{
        CtxID: childID,
        Ctx: inChild,
    })
    inChild.setParent(C)
    C.childrenMutex.Unlock()
}

// CtxGetChildByID returns the child Context with the match ID (or nil if not found).
func (C *Context) CtxGetChildByID(
	inChildID string,
) Ctx {

    var ctx Ctx

    C.childrenMutex.RLock()
    N := len(C.children)
    for i := 0; i < N; i++ {
        if C.children[i].CtxID == inChildID {
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

// CtxStatus returns an error if this Context is stopping or is stopped.
//
// THREADSAFE
func (C *Context) CtxStatus() error {

	if C.Ctx == nil {
		return Error(nil, ServiceShutdown, "context not running")
	}

	select {
        case <-C.Ctx.Done():
            return C.Ctx.Err()
	    default:
		    // still running baby!
	}

	return nil
}

// CtxRunning returns true if this Context has not yet been shutdown.
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

    C.CtxInitiateStop("fault limit reached")
}

func (C *Context) getParent() Ctx {
    return C.parent
}

func (C *Context) setParent(inNewParent Ctx) {
   if inNewParent != nil && C.parent != nil {
        panic("Context already has parent")
    }
    C.parent = inNewParent
}

func (C *Context) detachChild(
    inChild Ctx,
) {

    if inChild.getParent() != C {
        panic("Context child does not have matching parent")
    }

    inChild.setParent(nil)

    C.childrenMutex.Lock()
    N := len(C.children)
    for i := 0; i < N; i++ {
        if C.children[i].Ctx == inChild {
            N--
            copy(C.children[i:], C.children[i+1:N])
            C.children[N].Ctx = nil
            C.children = C.children[:N]
        }
    }
    C.childrenMutex.Unlock()

    if C.childStopping != nil {
        C.childStopping(inChild.(*Context))
    }
}


// ClientSession represents a client session over gRPC
type ClientSession struct {

	// SessionToken is handed back to remote pnode clients during authentication and used to retrieve a ClientSession
	//     in O(1) given a matching token string at later times.
	SessionToken string

	// PrevActivityTime says when this session was last accessed, used to know how long a session has been idle.
	PrevActivityTime int64

	// OnEndSession performs any closing/cleanup associated with the given session.
	// Note: this is called via goroutine, so concurrency considerations should be made.
	OnEndSession func(session *ClientSession, reason string)

	// Used to store an impl specific info
	Cookie interface{}
}

const (

	// ClientInactive reflects that a client session is ending because the client side has been inactive
	ClientInactive = "client inactive"

	// HostShuttingDown means that a client session is ending because the host machine/server is shutting down.
	HostShuttingDown = "host shutting down"

	// SessionTokenKey is the string name used to key the session token string (i.e. "session_token")
	SessionTokenKey = "session_token"
)

func genRandomSessionToken(N int) string {

	buf := make([]byte, N)

	crand.Read(buf)
	for i := 0; i < N; i++ {
		buf[i] = Base64pCharSet[buf[i]&0x3F]
	}

	return string(buf)
}

// SessionGroup takes a session token (a string) and hands back a ClientSession while ensuring concurrency safety.
type SessionGroup struct {
	sync.RWMutex

	table map[string]*ClientSession
}

// NewSessionGroup sets up a new SessionGroup for use
func NewSessionGroup() SessionGroup {
	return SessionGroup{
		table: map[string]*ClientSession{},
	}
}

/*
func (group *SessionGroup) Len() int {
    return len(group.byAccessTime)
}

func (group *SessionGroup) Less(i, j int) bool {
	return group.byAccessTime[i].PrevActivityTime.UnixSecs > group.byAccessTime[j].PrevActivityTime.UnixSecs
}

func (group *SessionGroup) Swap(i, j int) {
    byRank := group.byActivityRank

    byRank[i], byRank[j] = byRank[j], byRank[i]
	byRank[i].ActivityRank = i
	byRank[j].ActivityRank = j
}

func (group *SessionGroup) Push(x interface{}) {
	session := x.(*ClientSession)
	session.ActivityRank = len(*group)
	group.byActivityRank = append(group.byActivityRank, session)
}

func (group *SessionGroup) Pop() interface{} {
    byRank := group.byActivityRank

    last := len(byRank)-1
	session := byRank[last]
    group.byActivityRank = byRank[0:last]

	return session
}
*/

// ExtractSessionToken extracts the session_token from the given context
func ExtractSessionToken(ctx context.Context) (string, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || md == nil {
		return "", Errorf(nil, SessionTokenMissing, "no context metadata (or %v) found", SessionTokenKey)
	}

	strList := md[SessionTokenKey]
	if len(strList) < 1 {
		return "", Errorf(nil, SessionTokenMissing, "%v key not found", SessionTokenKey)
	}

	return strList[0], nil
}

// TransferSessionToken extracts and transfers the session token from the given context and returns a replacement with it appended to the metadata
func TransferSessionToken(ctx context.Context, md metadata.MD) (context.Context, error) {

	if md == nil {
		return nil, Errorf(nil, SessionTokenMissing, "no header/trailer (or %v) found", SessionTokenKey)
	}

	strList := md[SessionTokenKey]
	if len(strList) < 1 {
		return nil, Errorf(nil, SessionTokenMissing, "%v key not found", SessionTokenKey)
	}

	ctx2 := metadata.AppendToOutgoingContext(ctx, SessionTokenKey, strList[0])

	return ctx2, nil
}

// ApplyTokenOutgoingContext applies the given binary string to the given context, encoding it into a base64 string.
func ApplyTokenOutgoingContext(ctx context.Context, inToken []byte) context.Context {

	str := Base64p.EncodeToString(inToken)
	ctx2 := metadata.AppendToOutgoingContext(ctx, SessionTokenKey, str)

	return ctx2
}

// FetchSession extracts the session token string from the context, performs a session lookup, and returns the ClientSession object.
func (group *SessionGroup) FetchSession(ctx context.Context) (*ClientSession, error) {

	token, err := ExtractSessionToken(ctx)
	if err != nil {
		return nil, err
	}

	session := group.LookupSession(token, true)
	if session == nil {
		return nil, Errorf(nil, SessionTokenNotValid, "%s not valid", SessionTokenKey)
	}

	return session, nil
}

// LookupSession returns the ClientSession having the given session token.
func (group *SessionGroup) LookupSession(inSessionToken string, inBumpActivity bool) *ClientSession {

	if len(inSessionToken) > 0 {

		group.RLock()
		session := group.table[inSessionToken]
		group.RUnlock()

		if inBumpActivity && session != nil {
			session.PrevActivityTime = time.Now().Unix()
		}

		return session

	}

	return nil
}

// NewSession creates a new ClientSession and inserts the session token into the given context
func (group *SessionGroup) NewSession(
	ctx context.Context,
	inTokenOverride []byte,
) *ClientSession {

	session := &ClientSession{
		OnEndSession: func(*ClientSession, string) {},
	}

	if len(inTokenOverride) > 0 {
		session.SessionToken = Base64p.EncodeToString(inTokenOverride)
	} else {
		session.SessionToken = genRandomSessionToken(32)
	}

	group.InsertSession(session)

	tokenPair := metadata.Pairs(SessionTokenKey, session.SessionToken)
	grpc.SetTrailer(ctx, tokenPair)

	return session
}

// InsertSession inserts the given session into this SessionGroup -- THREADSAFE
func (group *SessionGroup) InsertSession(inSession *ClientSession) {

	inSession.PrevActivityTime = time.Now().Unix()

	group.Lock()
	group.table[inSession.SessionToken] = inSession
	group.Unlock()

}

// EndSession removes the given session from this SessionGroup -- THREADSAFE
func (group *SessionGroup) EndSession(inSessionToken string, inReason string) {

	group.Lock()
	session := group.table[inSessionToken]
	if session != nil {
		delete(group.table, inSessionToken)
	}
	group.Unlock()

	if session != nil {
		go session.OnEndSession(session, inReason)
	}
}

// EndInactiveSessions removes alls sessions inactive longer than the given time index -- THREADSAFE
func (group *SessionGroup) EndInactiveSessions(inExpirationTime int64) {

	var expired []*ClientSession

	// First, make a list to see if any have even expired
	group.RLock()
	for _, session := range group.table {
		if session.PrevActivityTime > inExpirationTime {
			expired = append(expired, session)
		}
	}
	group.RUnlock()

	// Only lock the session group for mutex write access if we need to remove items.
	if len(expired) > 0 {
		group.Lock()
		for _, session := range expired {
			delete(group.table, session.SessionToken)
		}
		group.Unlock()

		for _, i := range expired {
			go i.OnEndSession(i, ClientInactive)
		}
	}
}

// EndAllSessions ends all sessions
func (group *SessionGroup) EndAllSessions(inReason string) {

	group.Lock()
	oldTable := group.table
	group.table = make(map[string]*ClientSession)
	group.Unlock()

	for _, session := range oldTable {
		go session.OnEndSession(session, inReason)
	}

}

//
//
//
//
//
//
// Logger is an aid to logging and provides convenience functions
type Logger struct {
	hasPrefix bool
	logPrefix string
	logLabel  string
}

// SetLogLabel sets the label prefix for all entries logged.
func (l *Logger) SetLogLabel(inLabel string) {
	l.logLabel = inLabel
	l.hasPrefix = len(inLabel) > 0
	if l.hasPrefix {
		l.logPrefix = fmt.Sprintf("<%s> ", inLabel)
	}
}

// GetLogLabel gets the label last set via SetLogLabel()
func (l *Logger) GetLogLabel() string {
	return l.logLabel
}

// LogV returns true if logging is currently enabled for log verbose level.
func (l *Logger) LogV(inVerboseLevel int32) bool {
	return bool(klog.V(klog.Level(inVerboseLevel)))
}

// Info logs to the INFO log.
// Arguments are handled in the manner of fmt.Print(); a newline is appended if missing.
func (l *Logger) Info(inVerboseLevel int32, args ...interface{}) {
	logIt := true
	if inVerboseLevel > 0 {
		logIt = bool(klog.V(klog.Level(inVerboseLevel)))
	}

	if logIt {
		if l.hasPrefix {
			klog.InfoDepth(1, l.logPrefix, fmt.Sprint(args...))
		} else {
			klog.InfoDepth(1, args...)
		}
	}
}

// Infof logs to the INFO log.
// Arguments are handled in the manner of fmt.Printf(); a newline is appended if missing.
func (l *Logger) Infof(inV klog.Level, inFormat string, args ...interface{}) {
	logIt := true
	if inV > 0 {
		logIt = bool(klog.V(inV))
	}

	if logIt {
		if l.hasPrefix {
			klog.InfoDepth(1, l.logPrefix, fmt.Sprintf(inFormat, args...))
		} else {
			klog.InfoDepth(1, fmt.Sprintf(inFormat, args...))
		}
	}
}

// Warn logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Print(); a newline is appended if missing.
//
// Warnings are reserved for situations that indicate an inconsistency or an error that
// won't result in a departure of specifications, correctness, or expected behavior.
func (l *Logger) Warn(args ...interface{}) {
	{
		if l.hasPrefix {
			klog.WarningDepth(1, l.logPrefix, fmt.Sprint(args...))
		} else {
			klog.WarningDepth(1, args...)
		}
	}
}

// Warnf logs to the WARNING and INFO logs.
// Arguments are handled in the manner of fmt.Printf(); a newline is appended if missing.
//
// See comments above for Warn() for guidelines on errors vs warnings.
func (l *Logger) Warnf(inFormat string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.WarningDepth(1, l.logPrefix, fmt.Sprintf(inFormat, args...))
		} else {
			klog.WarningDepth(1, fmt.Sprintf(inFormat, args...))
		}
	}
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print(); a newline is appended if missing.
//
// Errors are reserved for situations that indicate an implementation deficiency, a
// corruption of data or resources, or an issue that if not addressed could spiral into deeper issues.
// Logging an error reflects that correctness or expected behavior is either broken or under threat.
func (l *Logger) Error(args ...interface{}) {
	{
		if l.hasPrefix {
			klog.ErrorDepth(1, l.logPrefix, fmt.Sprint(args...))
		} else {
			klog.ErrorDepth(1, args...)
		}
	}
}

// Errorf logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled in the manner of fmt.Print; a newline is appended if missing.
//
// See comments above for Error() for guidelines on errors vs warnings.
func (l *Logger) Errorf(inFormat string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.ErrorDepth(1, l.logPrefix, fmt.Sprintf(inFormat, args...))
		} else {
			klog.ErrorDepth(1, fmt.Sprintf(inFormat, args...))
		}
	}
}

// Fatalf logs to the FATAL, ERROR, WARNING, and INFO logs,
// Arguments are handled in the manner of fmt.Printf(); a newline is appended if missing.
func (l *Logger) Fatalf(inFormat string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.FatalDepth(1, l.logPrefix, fmt.Sprintf(inFormat, args...))
		} else {
			klog.FatalDepth(1, fmt.Sprintf(inFormat, args...))
		}
	}
}

// Fatalf -- see Fatalf (above)
func Fatalf(inFormat string, args ...interface{}) {
	gLogger.Fatalf(inFormat, args...)
}

var gLogger = Logger{}

//
//
//
//
//
//
// Buf is a flexible buffer designed for reuse.
type Buf struct {
	Unmarshaller

	Bytes []byte
}

//
//
//
//
//
//
// Unmarshal effectively copies the src buffer.
func (buf *Buf) Unmarshal(inSrc []byte) error {
	N := len(inSrc)
	if cap(buf.Bytes) < N {
		allocSz := ((N + 127) >> 7) << 7
		buf.Bytes = make([]byte, N, allocSz)
	} else {
		buf.Bytes = buf.Bytes[:N]
	}
	copy(buf.Bytes, inSrc)

	return nil
}

/*****************************************************
** Utility & Conversion Helpers
**/

// GetCommunityID returns the CommunityID for the given buffer
func GetCommunityID(in []byte) CommunityID {
	var out CommunityID

	copy(out[:], in)
	return out
}

/*

// Multiplex interleaves the bits of A and B such that a composite uint64 is formed.
//
// Generally, A and B are "counting-like" numbers that are associated with a unique issuance
// number (and are generally human readable).
// This ensures that the composite uint64 is also small and can be looked at by a human easily
// and can also be easily stored compressed.
// In PLAN, channel epoch IDs are generated from a memberID and their own self-issued ID value,
//     ensuring that epoch IDs can never collide (without having to use 20+ byte channel and epoch IDs).
func Multiplex(A, B uint32) uint64 {

    ax := uint64(A)
    bx := uint64(B) << 1

    place := byte(0)
    x := uint64(0)

    for (ax | bx) > 0 {
        x |= ((ax & 1) + (bx & 2)) << place
        ax >>= 1
        bx >>= 1
	    place += 2
    }

    return x
}

// Unplex is the inverse of Multiplex().
func Unplex(x uint64) (uint32, uint32) {

    xa := x
    xb := x >> 1

    place := byte(0)
    A := uint32(0)
    B := uint32(0)
    for xa > 0 {
        A |= uint32(xa & 1) << place
        B |= uint32(xb & 1) << place
        xa >>= 2
        xb >>= 2
        place++
    }

    return A, B
}

*/

// UseLocalDir ensures the dir pathname associated with PLAN exists and returns the final absolute pathname
// inSubDir can be any relative pathname
func UseLocalDir(inSubDir string) (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", Error(err, FileSysError, "failed to get current user dir")
	}

	pathname := usr.HomeDir
	pathname = path.Clean(path.Join(pathname, "_.plan"))

	if len(inSubDir) > 0 {
		pathname = path.Join(pathname, inSubDir)
	}

	err = os.MkdirAll(pathname, DefaultFileMode)
	if err != nil {
		return "", Error(err, FileSysError, "os.MkdirAll() failed")
	}

	return pathname, nil

}

// CreateNewDir creates the specified dir (and returns an error if the dir already exists)
//
// If inPath is absolute then inBasePath is ignored.
// Returns the effective pathname.
func CreateNewDir(inBasePath, inPath string) (string, error) {

	var pathname string

	if path.IsAbs(inPath) {
		pathname = inPath
	} else {
		pathname = path.Join(inBasePath, inPath)
	}

	if _, err := os.Stat(pathname); !os.IsNotExist(err) {
		return "", Errorf(nil, FailedToAccessPath, "for safety, the path '%s' must not already exist", pathname)
	}

	if err := os.MkdirAll(pathname, DefaultFileMode); err != nil {
		return "", err
	}

	return pathname, nil
}

var remapCharset = map[rune]rune{
	' ':  '-',
	'.':  '-',
	'?':  '-',
	'\\': '+',
	'/':  '+',
	'&':  '+',
}

// MakeFSFriendly makes a given string safe to use for a file system.
// If inSuffix is given, the hex encoding of those bytes are appended after "-"
func MakeFSFriendly(inName string, inSuffix []byte) string {

	var b strings.Builder
	for _, r := range inName {
		if replace, ok := remapCharset[r]; ok {
			if replace != 0 {
				b.WriteRune(replace)
			}
		} else {
			b.WriteRune(r)
		}
	}

	name := b.String()
	if len(inSuffix) > 0 {
		name = name + "-" + hex.EncodeToString(inSuffix)
	}

	return name
}




var (
	bytesT  = reflect.TypeOf(Bytes(nil))
)

// Bytes marshal/unmarshal as a JSON string with 0x prefix.
// The empty slice marshals as "0x".
type Bytes []byte

// MarshalText implements encoding.TextMarshaler
func (b Bytes) MarshalText() ([]byte, error) {
	out := make([]byte, len(b)*2+2)
    out[0] = '0'
    out[1] = 'x'
	hex.Encode(out[2:], b)
	return out, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(in []byte) error {
	if ! isString(in) {
		return errNonString(bytesT)
	}
	return wrapTypeError(b.UnmarshalText(in[1:len(in)-1]), bytesT)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *Bytes) UnmarshalText(input []byte) error {
	raw, err := checkText(input)
	if err != nil {
		return err
	}
	dec := make([]byte, len(raw)/2)
	if _, err = hex.Decode(dec, raw); err == nil {
		*b = dec
	}
	return err
}

// String returns the hex encoding of b.
func (b Bytes) String() string {
	out := make([]byte, len(b)*2+2)
    out[0] = '0'
    out[1] = 'x'
	hex.Encode(out[2:], b)
	return string(out)
}

func isString(input []byte) bool {
	return len(input) >= 2 && input[0] == '"' && input[len(input)-1] == '"'
}

func wrapTypeError(err error, typ reflect.Type) error {
	if _, ok := err.(*decError); ok {
		return &json.UnmarshalTypeError{Value: err.Error(), Type: typ}
	}
	return err
}

func errNonString(typ reflect.Type) error {
	return &json.UnmarshalTypeError{Value: "non-string", Type: typ}
}

func checkText(in []byte) ([]byte, error) {
    N := len(in)
	if N == 0 {
		return nil, nil // empty strings are allowed
	}
	if N >= 2 && in[0] == '0' && (in[1] == 'x' || in[1] == 'X') {
		in = in[2:]
        N -= 2
	} else {
		return nil, ErrMissingPrefix
	}
	if (N & 1) != 0 {
		return nil, ErrOddLength
	}
	return in, nil
}


const badNibble = ^uint64(0)

func decodeNibble(in byte) uint64 {
	switch {
	case in >= '0' && in <= '9':
		return uint64(in - '0')
	case in >= 'A' && in <= 'F':
		return uint64(in - 'A' + 10)
	case in >= 'a' && in <= 'f':
		return uint64(in - 'a' + 10)
	default:
		return badNibble
	}
}


const uintBits = 32 << (uint64(^uint(0)) >> 63)

// Errors
var (
	ErrEmptyString   = &decError{"empty hex string"}
	ErrSyntax        = &decError{"invalid hex string"}
	ErrMissingPrefix = &decError{"hex string without 0x prefix"}
	ErrOddLength     = &decError{"hex string of odd length"}
	ErrEmptyNumber   = &decError{"hex string \"0x\""}
	ErrLeadingZero   = &decError{"hex number with leading zero digits"}
	ErrUint64Range   = &decError{"hex number > 64 bits"}
	ErrUintRange     = &decError{fmt.Sprintf("hex number > %d bits", uintBits)}
	ErrBig256Range   = &decError{"hex number > 256 bits"}
)

type decError struct{ msg string }

func (err decError) Error() string { return err.msg }

