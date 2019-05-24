package plan


import (
    "context"
    "os"
	"os/user"
    "path"
    "strings"
    "encoding/hex"
    "sync"

    //crand "crypto/rand" 

    log "github.com/sirupsen/logrus"

    //"github.com/ethereum/go-ethereum/common/math"
)

// Blob is a convenience function that forms a ChID byte array from a ChID byte slice. 
func (chID ChID) Blob() ChIDBlob {

    var blob ChIDBlob
    copy(blob[:], chID)

    return blob
}


func (chID ChID) String() string {
    return Base64.EncodeToString(chID)
}

// AssignFromTID copies from the right-part of the given TID
func (chID ChID) AssignFromTID(tid TID) {
    copy(chID, tid[TIDSz - ChIDSz:])
}


// Blob is a convenience function that forms a TID byte array from a TID byte slice. 
func (tid TID) Blob() TIDBlob {

    var blob TIDBlob
    copy(blob[:], tid)

    return blob
}

func (tid TID) String() string {
    return Base64.EncodeToString(tid)
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
    if t < 1 {
        inTime = TimeFS(1)
    }

    if inTime < t || t < 1 {
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


