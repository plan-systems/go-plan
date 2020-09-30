package ctx

import (
	"fmt"

	"github.com/plan-systems/klog"
)

// Logger anstracts basic logging functions.
type Logger interface {
	SetLogLabel(label string)
	GetLogLabel() string
	GetLogPrefix() string
	LogV(verboseLevel int32) bool
	Info(verboseLevel int32, args ...interface{})
	Infof(verboseLevel int32, format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

//
// logger is an aid to logging and provides convenience functions
type logger struct {
	hasPrefix bool
	logPrefix string
	logLabel  string
}

// NewLogger creates and inits a new Logger with the given label.
func NewLogger(label string) Logger {
	l := &logger{}
	if label != "" {
		l.SetLogLabel(label)
	}
	return l
}

// SetLogLabel sets the label prefix for all entries logged.
func (l *logger) SetLogLabel(label string) {
	l.logLabel = label
	l.hasPrefix = len(label) > 0
	if l.hasPrefix {
		l.logPrefix = fmt.Sprintf("[%s] ", label)
	} else {
        l.logPrefix = ""
    }
}

// GetLogLabel returns the label last set via SetLogLabel()
func (l *logger) GetLogLabel() string {
	return l.logLabel
}

// GetLogPrefix returns the the text that prefixes all log messages for this context.
func (l *logger) GetLogPrefix() string {
	return l.logPrefix
}

// LogV returns true if logging is currently enabled for log verbose level.
func (l *logger) LogV(verboseLevel int32) bool {
	return klog.V(klog.Level(verboseLevel)).Enabled()
}

// Info logs to the INFO log.
// Arguments are handled like fmt.Print(); a newline is appended if missing.
//
// Verbose level conventions:
//   0. Enabled during production and field deployment.  Use this for important high-level events.
//   1. Enabled during testing and development. Use for high-level changes in state, mode, or connection.
//   2. Enabled during low-level debugging and troubleshooting.
func (l *logger) Info(verboseLevel int32, args ...interface{}) {
	if verboseLevel == 0 || klog.V(klog.Level(verboseLevel)).Enabled() {
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
// See comments above for Info() for guidelines for verboseLevel.
func (l *logger) Infof(verboseLevel int32, format string, args ...interface{}) {
	if verboseLevel == 0 || klog.V(klog.Level(verboseLevel)).Enabled() {
		if l.hasPrefix {
			klog.InfoDepth(1, l.logPrefix, fmt.Sprintf(format, args...))
		} else {
			klog.InfoDepth(1, fmt.Sprintf(format, args...))
		}
	}
}

// Warn logs to the WARNING and INFO logs.
// Arguments are handled like fmt.Print(); a newline is appended if missing.
//
// Warnings are reserved for situations that indicate an inconsistency or an error that
// won't result in a departure of specifications, correctness, or expected behavior.
func (l *logger) Warn(args ...interface{}) {
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
func (l *logger) Warnf(format string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.WarningDepth(1, l.logPrefix, fmt.Sprintf(format, args...))
		} else {
			klog.WarningDepth(1, fmt.Sprintf(format, args...))
		}
	}
}

// Error logs to the ERROR, WARNING, and INFO logs.
// Arguments are handled like fmt.Print(); a newline is appended if missing.
//
// Errors are reserved for situations that indicate an implementation deficiency, a
// corruption of data or resources, or an issue that if not addressed could spiral into deeper issues.
// Logging an error reflects that correctness or expected behavior is either broken or under threat.
func (l *logger) Error(args ...interface{}) {
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
func (l *logger) Errorf(format string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.ErrorDepth(1, l.logPrefix, fmt.Sprintf(format, args...))
		} else {
			klog.ErrorDepth(1, fmt.Sprintf(format, args...))
		}
	}
}

// Fatalf logs to the FATAL, ERROR, WARNING, and INFO logs,
// Arguments are handled like fmt.Printf(); a newline is appended if missing.
func (l *logger) Fatalf(format string, args ...interface{}) {
	{
		if l.hasPrefix {
			klog.FatalDepth(1, l.logPrefix, fmt.Sprintf(format, args...))
		} else {
			klog.FatalDepth(1, fmt.Sprintf(format, args...))
		}
	}
}