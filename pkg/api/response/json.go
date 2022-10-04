package response

import (
	"encoding/json"
)

type Json struct {
	code  int
	body  interface{}
	jsonp string
}

func NewJson(code int, body interface{}, jsonp string) *Json {
	return &Json{
		code:  code,
		body:  body,
		jsonp: jsonp,
	}
}

func (r *Json) Code() int {
	return r.code
}

func (r *Json) Close() {
	//NOOP
	return
}

func (r *Json) Body() (buf []byte, err error) {
	buf, err = json.Marshal(r.body)
	if r.jsonp == "" || err != nil {
		return buf, err
	}
	buf = append([]byte(r.jsonp+"("), buf...)
	buf = append(buf, byte(')'))
	return buf, err
}

func (r *Json) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/json"}
	if r.jsonp != "" {
		headers["content-type"] = "text/javascript"
	}
	return headers
}
