package ski

import (
	"fmt"
)

// Error makes our custom error type conform to a standard Go error
func (err *Err) Error() string {
	codeStr, exists := ErrCode_name[int32(err.Code)]
	if exists == false {
		codeStr = ErrCode_name[int32(ErrCode_UnnamedErr)]
	}

	if len(err.Msg) == 0 {
		return codeStr
	}

	return codeStr + ": " + err.Msg
}

// Err returns a Err with the given error code
func (code ErrCode) Err() error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &Err{
		Code: code,
	}
}

// ErrWithMsg returns a Err with the given error code and msg set.
func (code ErrCode) ErrWithMsg(msg string) error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &Err{
		Code: code,
		Msg:  msg,
	}
}

// ErrWithMsgf returns a Err with the given error code and formattable msg set.
func (code ErrCode) ErrWithMsgf(msgFormat string, msgArgs ...interface{}) error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &Err{
		Code: code,
		Msg:  fmt.Sprintf(msgFormat, msgArgs...),
	}
}

// Wrap returns a Err with the given error code and "cause" error
func (code ErrCode) Wrap(cause error) error {
	if cause == nil {
		return nil
	}
	return &Err{
		Code: code,
		Msg:  cause.Error(),
	}
}

// IsError tests if the given error is a Err error code (below). 
// If err == nil, this returns false.
func IsError(err error, errCodes ...ErrCode) bool {
	if err == nil {
		return false
	}
	if perr, ok := err.(*Err); ok && perr != nil {
		for _, errCode := range errCodes {
			if perr.Code == errCode {
				return true
			}
		}
	}

	return false
}
