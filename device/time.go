package device

import (
	"time"
)

// TimeFS is the UTC in 1/1<<16 seconds elapsed since Jan 1, 1970 UTC ("FS" = fractional seconds)
//
// timeFS := TimeNowFS()
//
// Shifting this right 16 bits will yield stanard Unix time.
// This means there are 47 bits dedicated for seconds, implying max timestamp of 4.4 million years.
type TimeFS int64

// TimeNowFS returns the current time (a standard unix UTC timestamp in 1/1<<16 seconds)
func TimeNowFS() TimeFS {
	t := time.Now()

	timeFS := t.Unix() << 16
	frac := uint16((2199 * (uint32(t.Nanosecond()) >> 10)) >> 15)
	return TimeFS(timeFS | int64(frac))
}
