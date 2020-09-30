package ctx

import (
	//"fmt"
)

type ctxErr struct{ msg string }
func (err *ctxErr) Error() string {
    return err.msg
}

// Errors
var (
    // File system related
    DirAlreadyExists = &ctxErr{"for safety, dir must not already exist"}

    // ctx.Context
    ErrCtxNotRunning = &ctxErr{"context not running"}

	// session/tokens
	ErrSessTokenMissing  = &ctxErr{"session token was not found in the request context"}
	ErrSessTokenNotValid = &ctxErr{"session token was not valid"}
)
