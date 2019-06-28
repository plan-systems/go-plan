package tools

import (
	//"fmt"
)

type toolsErr struct{ msg string }
func (err *toolsErr) Error() string {
    return err.msg
}

// Errors
var (
    // File system related
    DirAlreadyExists = &toolsErr{"for safety, dir must not already exist"}

    // tools.Context
    ErrCtxNotRunning = &toolsErr{"context not running"}

    // Parsing/Encoding
	ErrEmptyString   = &toolsErr{"empty hex string"}
	ErrSyntax        = &toolsErr{"invalid hex string"}
	ErrMissingPrefix = &toolsErr{"hex string without 0x prefix"}
	ErrOddLength     = &toolsErr{"hex string of odd length"}
	ErrEmptyNumber   = &toolsErr{"hex string \"0x\""}
	ErrLeadingZero   = &toolsErr{"hex number with leading zero digits"}
	ErrUint64Range   = &toolsErr{"hex number > 64 bits"}
	ErrBig256Range   = &toolsErr{"hex number > 256 bits"}

	// session/tokens
	ErrSessTokenMissing  = &toolsErr{"session token was not found in the request context"}
	ErrSessTokenNotValid = &toolsErr{"session token was not valid"}
)
