package plan


import (
    "bytes"
    "context"
    "fmt"
    "os"
	"os/user"
    "path"
    "strings"
    "encoding/hex"
    "sync"
    "time"


    crand "crypto/rand" 

    log "github.com/sirupsen/logrus"
    "github.com/plan-systems/klog"

	"google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
)

func init() {

    klog.InitFlags(nil)
    klog.SetFormatter(&klog.FmtConstWidth{
        FileNameCharWidth: 20,
        UseColor: true,
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
    copy(chID, tid[TIDSz - ChIDSz:])
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
    tid[6] = byte(inTime >>  8)
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
        tid[6] = byte(inTime >>  8)
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

// Flow is a service helper.
type Flow struct {

    Ctx              context.Context
    ctxCancel        context.CancelFunc

    Desc             string

    ShutdownComplete sync.WaitGroup
    ShutdownReason   string
    shutdownMutex    sync.Mutex

    FaultLog         []error
    FaultLimit       int
    Log              *log.Logger 
    logMutex         sync.Mutex

}

// Startup blocks until this Context is started up or stopped (if an err is returned).
func (F *Flow) Startup(
    inCtx              context.Context,
    inDesc             string,
    onInternalStartup  func() error,
    onInternalShutdown func(),
) error {

    F.ShutdownComplete.Wait()
    F.ShutdownComplete.Add(1)

    F.FaultLimit = 3
    F.Log = log.StandardLogger()
    F.Desc = inDesc
    F.Ctx, F.ctxCancel = context.WithCancel(inCtx)

    err := onInternalStartup()
    if err != nil {
        F.InitiateShutdown(inDesc + " internal startup failed")
    }

    go func() {

        select {
            case <- F.Ctx.Done():
        }

        onInternalShutdown()
  
        F.shutdownMutex.Lock()
        F.ctxCancel = nil
        F.shutdownMutex.Unlock()

        F.ShutdownComplete.Done()
    }()


    if err != nil {
        F.ShutdownComplete.Wait()

        F.Log.WithError(err).Warnf("%v startup failed", F.Desc)
    }

    return err
}

// CheckStatus returns an error if this Service is shut down or shutting down.
//
// THREADSAFE
func (F *Flow) CheckStatus() error {

    if F.Ctx == nil {
        return Error(nil, ServiceShutdown, "context not running") 
    }

    select {
        case <-F.Ctx.Done():
            return F.Ctx.Err()
        default:
            // still running baby!
    }

    return nil
}


// IsRunning returns true if this Context has not yet been shutdown.
//
// THREADSAFE
func (F *Flow) IsRunning() bool {

    if F.Ctx == nil {
        return false 
    }

    select {
        case <-F.Ctx.Done():
            return false
        default:
    }

    return true
}

// InitiateShutdown initiates a polite shutdown of this Context, returning immediately after.
//
// If Shutdown() has already been called, behavior is consistent but inReason will be dropped.
//
// THREADSAFE
func (F *Flow) InitiateShutdown(
    inReason string,
) bool {

    initiated := false

    F.shutdownMutex.Lock()
    if F.IsRunning() && F.ctxCancel != nil {
        F.ShutdownReason = inReason
        F.Log.Infof("Initiating shutdown for %s: %s", F.Desc, F.ShutdownReason)
        F.ctxCancel()
        F.ctxCancel = nil
        initiated = true
    }
    F.shutdownMutex.Unlock()


    return initiated
}

// Shutdown calls InitiateShutdown() and blocks until complete.
//
// THREADSAFE
func (F *Flow) Shutdown(
    inReason string,
) {

    initiated := F.InitiateShutdown(inReason)

    F.ShutdownComplete.Wait()

    if initiated {
        F.Log.Infof("Shutdown complete for %s", F.Desc)
    }
}



// FilterFault is called when the given error has occurred an represents an unexpected fault that alone doesn't
// justify an emergency condition. (e.g. a db error while accessing a record).  Call this on unexpected errors.
//
// if inErr is nil, this is equivalent to calling CheckStatus()
//
// Returns non-nil if the caller should abort whatever it's doing and anticipate a shutdown.
//
// THREADSAFE
func (F *Flow) FilterFault(inErr error) error {

    if inErr == nil {
        return F.CheckStatus()
    }

    F.logMutex.Lock()
    F.Log.WithError(inErr).Warn("fault occurred")
    F.FaultLog = append(F.FaultLog, inErr)
    faultCount := len(F.FaultLog)
    F.logMutex.Unlock()

    if faultCount < F.FaultLimit {
        return nil
    }

    if F.IsRunning() {
        go F.Shutdown("fault limit exceeded")
    }

    return inErr
}

// LogErr logs a warning for the given error with the given description.  
// If inErr == nil, this function has no effect
func (F *Flow) LogErr(inErr error, inDesc string) {
    if inErr != nil {
        F.Log.WithError(inErr).Warn(inDesc)
    }
}

// ContextWorker is used by Context as the model of subcontrol.
type ContextWorker interface {

    CtxInternalStart() error
    CtxInternalStop()
}

// Context is a service helper.
type Context struct {
    Logger

    worker          ContextWorker

    Ctx             context.Context
    ctxCancel       context.CancelFunc

    StopComplete    sync.WaitGroup
    stopMutex       sync.Mutex
    stopReason      string

    FaultLog        []error
    FaultLimit      int

}

// CtxStart blocks until this Context is started up or stopped (if an err is returned).
func (C *Context) CtxStart(
    inParentCtx context.Context,
    inDesc      string,
    inWorker    ContextWorker,              
) error {

    C.SetLogLabel(inDesc)
    C.worker = inWorker

    C.StopComplete.Wait()
    C.StopComplete.Add(1)

    C.FaultLimit = 3
    C.Ctx, C.ctxCancel = context.WithCancel(inParentCtx)

    err := C.worker.CtxInternalStart()
    if err != nil {
        C.Errorf("CtxStart failed: %v", err)
        C.CtxInitiateStop("CtxStart failed")
    }

    go func() {

        select {
            case <- C.Ctx.Done():
        }

        C.Info(2, "plan.Context done: ", C.Ctx.Err())

        C.worker.CtxInternalStop()
  
        C.stopMutex.Lock()
        C.ctxCancel = nil
        C.stopMutex.Unlock()

        C.Info(2, "plan.Context stopped")

        C.StopComplete.Done()
    }()


    if err != nil {
        C.StopComplete.Wait()
    }

    return err
}


// CtxStatus returns an error if this Context is shut down or shutting down.
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

// CtxInitiateStop initiates a polite shutdown of this Context, returning immediately after.
//
// If Shutdown() has already been called, behavior is consistent but inReason will be dropped.
//
// THREADSAFE
func (C *Context) CtxInitiateStop(
    inReason string,
) bool {

    initiated := false

    C.stopMutex.Lock()
    if C.CtxRunning() && C.ctxCancel != nil {
        C.stopReason = inReason
        C.Infof(2, "initiating CtxStop (%s)", C.stopReason)
        C.ctxCancel()
        C.ctxCancel = nil
        initiated = true
    }
    C.stopMutex.Unlock()

    return initiated
}

// CtxStop calls InitiateShutdown() and blocks until complete.
//
// Retruns true if this call initiated the shutdown (vs another cause)
//
// THREADSAFE
func (C *Context) CtxStop(
    inReason string,
) bool {
    initiated := C.CtxInitiateStop(inReason)

    C.StopComplete.Wait()

    return initiated
}


// ClientSession represents a client session over gRPC
type ClientSession struct {         

    // SessionToken is handed back to remote pnode clients during authentication and used to retrieve a ClientSession
    //     in O(1) given a matching token string at later times.
    SessionToken            string

    // PrevActivityTime says when this session was last accessed, used to know how long a session has been idle.
    PrevActivityTime        int64

    // OnEndSession performs any closing/cleanup associated with the given session.  
    // Note: this is called via goroutine, so concurrency considerations should be made.
    OnEndSession            func(session *ClientSession, reason string)

    // Used to store an impl specific info
    Cookie                  interface{}
}



const (

    // ClientInactive reflects that a client session is ending because the client side has been inactive
    ClientInactive          = "client inactive"

    // HostShuttingDown means that a client session is ending because the host machine/server is shutting down.
    HostShuttingDown        = "host shutting down"

    // SessionTokenKey is the string name used to key the session token string (i.e. "session_token")
    SessionTokenKey         = "session_token"
)





func genRandomSessionToken(N int) string {

    buf := make([]byte, N)

    crand.Read(buf)
    for i := 0; i < N; i++ {
        buf[i] = Base64pCharSet[buf[i] & 0x3F]
    }
    
    return string( buf )
}










// SessionGroup takes a session token (a string) and hands back a ClientSession while ensuring concurrency safety.
type SessionGroup struct {
    sync.RWMutex

    table                   map[string]*ClientSession

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
    if ! ok || md == nil {
        return "", Errorf(nil, SessionTokenMissing, "no context metadata (or %v) found", SessionTokenKey)
    }

    strList := md[SessionTokenKey]; 
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

    strList := md[SessionTokenKey]; 
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
        OnEndSession: func(*ClientSession, string) { },
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

// Buf is a flexible buffer designed for reuse.
type Buf struct {
    Unmarshaller

    Bytes []byte
}

// Unmarshal effectively copies the src buffer.
func (buf *Buf) Unmarshal(inSrc []byte) error {
    N := len(inSrc)
    if cap(buf.Bytes) < N {
        allocSz := ((N+127)>>7)<<7
        buf.Bytes = make([]byte, N, allocSz)
    } else {
        buf.Bytes = buf.Bytes[:N]
    }
    copy(buf.Bytes, inSrc)

    return nil
}


/*
type SessionList struct {

    orderMatters bool
    items        []interface{}
    itemsMutex   sync.Mutex
}


   N := len(mgr.List)
    for i := 0; i < N; i++ {
        if mgr.List[i] == inSess {
            N--
            mgr.List[i] = mgr.List[N]
            mgr.List[N] = nil
            mgr.List = mgr.List[:N]
            break
        }
    }
*/


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
// GetChannelID returns the channel ID for the given buffer
func GetChannelID(in []byte) ChannelID {
	var out ChannelID

	copy(out[:], in)
	return out
}
*/


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

    if _, err := os.Stat(pathname); ! os.IsNotExist(err) {
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


