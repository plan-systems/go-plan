package bufs

type encodingErr struct {
	msg string
}

func (err *encodingErr) Error() string {
	return err.msg
}

// Errors
var (

	// Parsing/Encoding
	ErrSyntax = &encodingErr{"invalid hex string"}
)
