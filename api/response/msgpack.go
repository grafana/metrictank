package response

import (
	"github.com/tinylib/msgp/msgp"
)

type Msgpack struct {
	code int
	body msgp.Marshaler
	buf  []byte
}

func NewMsgpack(code int, body msgp.Marshaler) *Msgpack {
	return &Msgpack{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *Msgpack) Code() int {
	return r.code
}

func (r *Msgpack) Close() {
	BufferPool.Put(r.buf)
}

func (r *Msgpack) Body() ([]byte, error) {
	var err error
	r.buf, err = r.body.MarshalMsg(r.buf)
	return r.buf, err
}

func (r *Msgpack) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/x-msgpack"}
	return headers
}
