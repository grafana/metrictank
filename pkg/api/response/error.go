package response

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type Error interface {
	// HTTPStatusCode could have just been `Code`, but that would make it accidentally share an interface with gocql and lead to nonsense HTTP codes
	// See https://github.com/grafana/metrictank/issues/1678
	HTTPStatusCode() int
	Error() string
}

type ErrorResp struct {
	code int
	err  string
}

func WrapError(e error) *ErrorResp {
	if err, ok := e.(*ErrorResp); ok {
		return err
	}
	resp := &ErrorResp{
		err:  e.Error(),
		code: http.StatusInternalServerError,
	}
	if err := Error(nil); errors.As(e, &err) {
		resp.code = err.HTTPStatusCode()
	}
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

	if err := Error(nil); errors.As(e, &err) {
		resp.code = err.HTTPStatusCode()
	}
	return resp
}

func NewError(code int, err string) *ErrorResp {
	return &ErrorResp{
		code: code,
		err:  err,
	}
}

func Errorf(code int, format string, a ...interface{}) *ErrorResp {
	return &ErrorResp{
		code: code,
		err:  fmt.Sprintf(format, a...),
	}
}

func (r *ErrorResp) Error() string {
	return r.err
}

func (r *ErrorResp) HTTPStatusCode() int {
	return r.code
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

const HttpClientClosedRequest = 499

var RequestCanceledErr = NewError(HttpClientClosedRequest, "request canceled")
