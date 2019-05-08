package plan


import (
    "context"
    "os"
	"os/user"
    "path"
    "strings"
    "encoding/hex"
    "sync"
    //"math/big"

    log "github.com/sirupsen/logrus"

    //"github.com/ethereum/go-ethereum/common/math"
)

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


// IsRunning returns true if this Context has not shutdown or shutting down.
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
// if inErr == nil, this is equivalent to calling CheckStatus()
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


