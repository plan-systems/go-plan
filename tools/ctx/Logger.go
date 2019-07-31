package ctx

import (
	"fmt"
	"github.com/plan-systems/klog"
)

func init() {

	klog.InitFlags(nil)
	klog.SetFormatter(&klog.FmtConstWidth{
		FileNameCharWidth: 20,
		UseColor: true,
	})

}

//
// Logger is an aid to logging and provides convenience functions
type Logger struct {
	hasPrefix bool
	logPrefix string
	logLabel  string
}

// Fatalf -- see Fatalf (above)
func Fatalf(inFormat string, args ...interface{}) {
	gLogger.Fatalf(inFormat, args...)
}

var gLogger = Logger{}

// SetLogLabel sets the label prefix for all entries logged.
func (l *Logger) SetLogLabel(inLabel string) {
	l.logLabel = inLabel
	l.hasPrefix = len(inLabel) > 0
	if l.hasPrefix {
		l.logPrefix = fmt.Sprintf("%s>> ", inLabel)
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
// Arguments are handled like fmt.Print(); a newline is appended if missing.
//
// Verbose level conventions:
//   0. Enabled during production and field deployment.  Use this for important high-level info.
//   1. Enabled during testing and development. Use for high-level changes in state, mode, or connection.
//   2. Enabled during low-level debugging and troubleshooting.
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
// Arguments are handled like fmt.Printf(); a newline is appended if missing.
//
// See comments above for Info() for guidelines for inVerboseLevel.
func (l *Logger) Infof(inVerboseLevel int32, inFormat string, args ...interface{}) {
	logIt := true
	if inVerboseLevel > 0 {
		logIt = bool(klog.V(klog.Level(inVerboseLevel)))
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
// Arguments are handled like fmt.Print(); a newline is appended if missing.
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
// Arguments are handled like fmt.Printf(); a newline is appended if missing.
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
// Arguments are handled like fmt.Print(); a newline is appended if missing.
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
// Arguments are handled like fmt.Print; a newline is appended if missing.
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
// Arguments are handled like fmt.Printf(); a newline is appended if missing.
func (l *Logger) Fatalf(inFormat string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.FatalDepth(1, l.logPrefix, fmt.Sprintf(inFormat, args...))
		} else {
			klog.FatalDepth(1, fmt.Sprintf(inFormat, args...))
		}
	}
}
