package response

type Picklable interface {
	Pickle([]byte) ([]byte, error)
}

type Pickle struct {
	code int
	body Picklable
	buf  []byte
}

func NewPickle(code int, body Picklable) *Pickle {
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
	var err error
	r.buf, err = r.body.Pickle(r.buf)
	return r.buf, err
}

func (r *Pickle) Headers() (headers map[string]string) {
	return map[string]string{"content-type": "application/pickle"}
}
