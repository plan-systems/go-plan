package repo

import (
	"fmt"
)

// Error makes our custom error type conform to a standard Go error
func (err *ReqErr) Error() string {
	codeStr, exists := ErrCode_name[int32(err.Code)]
	if exists == false {
		codeStr = ErrCode_name[int32(ErrCode_UnnamedErr)]
	}

	if len(err.Msg) == 0 {
		return codeStr
	}

	return codeStr + ": " + err.Msg
}

// Err returns a ReqErr with the given error code
func (code ErrCode) Err() error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &ReqErr{
		Code: code,
	}
}

// ErrWithMsg returns a ReqErr with the given error code and msg set.
func (code ErrCode) ErrWithMsg(msg string) error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &ReqErr{
		Code: code,
		Msg:  msg,
	}
}

// ErrWithMsgf returns a ReqErr with the given error code and formattable msg set.
func (code ErrCode) ErrWithMsgf(msgFormat string, msgArgs ...interface{}) error {
	if code == ErrCode_NoErr {
		return nil
	}
	return &ReqErr{
		Code: code,
		Msg:  fmt.Sprintf(msgFormat, msgArgs...),
	}
}

// Wrap returns a ReqErr with the given error code and "cause" error
func (code ErrCode) Wrap(cause error) error {
	if cause == nil {
		return nil
	}
	return &ReqErr{
		Code: code,
		Msg:  cause.Error(),
	}
}
