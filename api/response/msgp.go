package response

import (
	"github.com/tinylib/msgp/msgp"
)

type Msgp struct {
	code int
	body msgp.Marshaler
	buf  []byte
}

func NewMsgp(code int, body msgp.Marshaler) *Msgp {
	return &Msgp{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *Msgp) Code() int {
	return r.code
}

func (r *Msgp) Close() {
	BufferPool.Put(r.buf)
}

func (r *Msgp) Body() ([]byte, error) {
	var err error
	r.buf, err = r.body.MarshalMsg(r.buf)
	return r.buf, err
}

func (r *Msgp) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/msgpack"}
	return headers
}

type MsgpArray struct {
	code int
	body []msgp.Marshaler
	buf  []byte
}

func NewMsgpArray(code int, body []msgp.Marshaler) *MsgpArray {
	return &MsgpArray{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *MsgpArray) Code() int {
	return r.code
}

func (r *MsgpArray) Close() {
	BufferPool.Put(r.buf)
}

func (r *MsgpArray) Body() ([]byte, error) {
	var err error
	for _, b := range r.body {
		r.buf, err = b.MarshalMsg(r.buf)
		if err != nil {
			return r.buf, err
		}
	}
	return r.buf, err
}

func (r *MsgpArray) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/msgpack"}
	return headers
}
