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
) {

    F.shutdownMutex.Lock()
    if F.IsRunning() && F.ctxCancel != nil {
        F.ShutdownReason = inReason
        F.Log.Infof("stopping %s: %s", F.Desc, F.ShutdownReason)
        F.ctxCancel()
        F.ctxCancel = nil
    }
    F.shutdownMutex.Unlock()

}

// Shutdown calls InitiateShutdown() and blocks until complete.
//
// THREADSAFE
func (F *Flow) Shutdown(
    inReason string,
) {

    F.InitiateShutdown(inReason)

    F.ShutdownComplete.Wait()
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






/*****************************************************
** Utility & Conversion Helpers
**/

// GetCommunityID returns the CommunityID for the given buffer
func GetCommunityID(in []byte) CommunityID {

	var out CommunityID

	overhang := CommunityIDSz - len(in)
	if overhang < 0 {
		in = in[-overhang:]
		overhang = 0
	}

	copy(out[overhang:], in)
	return out
}

/*

// GetKeyID returns the KeyID for the given buffer
func GetKeyID(in []byte) KeyID {

	var out KeyID

	overhang := KeyIDSz - len(in)
	if overhang < 0 {
		in = in[-overhang:]
		overhang = 0
	}

	copy(out[overhang:], in)
	return out
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
func CreateNewDir(inBasePath, inPath string) error {

    var pathname string

    if path.IsAbs(inPath) {
        pathname = inPath
    } else {
        pathname = path.Join(inBasePath, inPath)
    }

    if _, err := os.Stat(pathname); ! os.IsNotExist(err) {
        return Errorf(nil, FailedToAccessPath, "for safety, the path '%s' must not already exist", pathname)
    }

    err := os.MkdirAll(pathname, DefaultFileMode)
    if err != nil { return err }

    return nil
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


