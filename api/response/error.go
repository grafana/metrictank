package response

type Error struct {
	code int
	err  error
}

func NewError(code int, err error) *Error {
	return &Error{
		code: code,
		err:  err,
	}
}

func (r *Error) Code() int {
	return r.code
}

func (r *Error) Close() {
	return
}

func (r *Error) Body() ([]byte, error) {
	return []byte(r.err.Error()), nil
}

func (r *Error) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "text/plain"}
	return headers
}
