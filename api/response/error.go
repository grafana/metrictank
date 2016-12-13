package response

type Error interface {
	Code() int
	Error() string
}

type ErrorResp struct {
	code int
	err  string
}

func WrapError(e error) *ErrorResp {
	if _, ok := e.(*ErrorResp); ok {
		return e.(*ErrorResp)
	}
	resp := &ErrorResp{
		err:  e.Error(),
		code: 500,
	}
	if _, ok := e.(Error); ok {
		resp.code = e.(Error).Code()
	}
	return resp
}

func NewError(code int, err string) *ErrorResp {
	return &ErrorResp{
		code: code,
		err:  err,
	}
}

func (r *ErrorResp) Error() string {
	return r.err
}

func (r *ErrorResp) Code() int {
	return r.code
}

func (r *ErrorResp) Close() {
	return
}

func (r *ErrorResp) Body() ([]byte, error) {
	return []byte(r.err), nil
}

func (r *ErrorResp) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "text/plain"}
	return headers
}
