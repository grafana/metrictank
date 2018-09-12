package response

import (
	"encoding/json"
	"net/http"
	"runtime/debug"

	"github.com/raintank/worldping-api/pkg/log"
)

type Error interface {
	Code() int
	Error() string
}

type ErrorResp struct {
	code int
	err  string
}

func WrapError(e error) *ErrorResp {
	if err, ok := e.(*ErrorResp); ok {
		err.ValidateAndFixCode()
		return err
	}
	resp := &ErrorResp{
		err:  e.Error(),
		code: http.StatusInternalServerError,
	}
	if _, ok := e.(Error); ok {
		resp.code = e.(Error).Code()
	}

	resp.ValidateAndFixCode()
	return resp
}

type TagDBError struct {
	Error string `json:"error"`
}

// graphite's http tagdb client requires a specific error format
func WrapErrorForTagDB(e error) *ErrorResp {
	b, err := json.Marshal(TagDBError{Error: e.Error()})
	if err != nil {
		return &ErrorResp{
			err:  "{\"error\": \"failed to encode error message\"}",
			code: http.StatusInternalServerError,
		}
	}

	resp := &ErrorResp{
		err:  string(b),
		code: http.StatusInternalServerError,
	}

	if _, ok := e.(Error); ok {
		resp.code = e.(Error).Code()
	}

	resp.ValidateAndFixCode()
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

func (r *ErrorResp) ValidateAndFixCode() {
	// 599 is max HTTP status code
	if r.code > 599 {
		log.Warn("Encountered invalid HTTP status code %d, printing stack", r.code)
		debug.PrintStack()
		r.code = http.StatusInternalServerError
	}
}

var RequestCanceledErr = NewError(499, "request canceled")
