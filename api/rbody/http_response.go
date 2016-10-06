package rbody

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/tinylib/msgp/msgp"
)

var ErrMetricNotFound = errors.New("metric not found")

func WriteResponse(w *middleware.Context, resp Response) {
	body, err := resp.Body()
	if err != nil {
		w.Error(http.StatusInternalServerError, err.Error())
		return
	}
	for k, v := range resp.Headers() {
		w.Header().Set(k, v)
	}
	w.WriteHeader(resp.Code())
	w.Write(body)
	return
}

type Response interface {
	Code() int
	Body() ([]byte, error)
	Headers() map[string]string
}

type JsonResponse struct {
	code  int
	body  interface{}
	jsonp string
}

func NewJsonResponse(code int, body interface{}, jsonp string) *JsonResponse {
	return &JsonResponse{
		code:  code,
		body:  body,
		jsonp: jsonp,
	}
}

func (r *JsonResponse) Code() int {
	return r.code
}

func (r *JsonResponse) Body() (buf []byte, err error) {
	buf, err = json.Marshal(r.body)
	if r.jsonp == "" || err != nil {
		return buf, err
	}
	buf = append([]byte(r.jsonp+"("), buf...)
	buf = append(buf, byte(')'))
	return buf, err
}

func (r *JsonResponse) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/json"}
	if r.jsonp != "" {
		headers["content-type"] = "text/javascript"
	}
	return headers
}

type ErrorResponse struct {
	code int
	err  error
}

func NewErrorResponse(code int, err error) *ErrorResponse {
	return &ErrorResponse{
		code: code,
		err:  err,
	}
}

func (r *ErrorResponse) Code() int {
	return r.code
}

func (r *ErrorResponse) Body() ([]byte, error) {
	return []byte(r.err.Error()), nil
}

func (r *ErrorResponse) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "text/plain"}
	return headers
}

type MsgpResponse struct {
	code int
	body msgp.Marshaler
}

func NewMsgpResponse(code int, body msgp.Marshaler) *MsgpResponse {
	return &MsgpResponse{
		code: code,
		body: body,
	}
}

func (r *MsgpResponse) Code() int {
	return r.code
}

func (r *MsgpResponse) Body() (buf []byte, err error) {
	return r.body.MarshalMsg(buf)
}

func (r *MsgpResponse) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/msgpack"}
	return headers
}

type MsgpArrayResponse struct {
	code int
	body []msgp.Marshaler
}

func NewMsgpArrayResponse(code int, body []msgp.Marshaler) *MsgpArrayResponse {
	return &MsgpArrayResponse{
		code: code,
		body: body,
	}
}

func (r *MsgpArrayResponse) Code() int {
	return r.code
}

func (r *MsgpArrayResponse) Body() (buf []byte, err error) {
	for _, b := range r.body {
		buf, err = b.MarshalMsg(buf)
		if err != nil {
			return
		}
	}
	return
}

func (r *MsgpArrayResponse) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/msgpack"}
	return headers
}
