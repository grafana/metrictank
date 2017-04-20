package response

import (
	"bytes"
	pickle "github.com/kisielk/og-rek"
)

type Pickle struct {
	code int
	body interface{}
	buf  []byte
}

func NewPickle(code int, body interface{}) *Pickle {
	return &Pickle{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *Pickle) Code() int {
	return r.code
}

func (r *Pickle) Close() {
	BufferPool.Put(r.buf)
}

func (r *Pickle) Body() ([]byte, error) {
	buffer := bytes.NewBuffer(r.buf)
	encoder := pickle.NewEncoder(buffer)
	err := encoder.Encode(r.body)
	return buffer.Bytes(), err
}

func (r *Pickle) Headers() (headers map[string]string) {
	return map[string]string{"content-type": "application/pickle"}
}
