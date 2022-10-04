package response

type Csvable interface {
	Csv([]byte) ([]byte, error)
}

type Csv struct {
	code int
	body Csvable
	buf  []byte
}

func NewCsv(code int, body Csvable) *Csv {
	return &Csv{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *Csv) Code() int {
	return r.code
}

func (r *Csv) Close() {
	BufferPool.Put(r.buf)
}

func (r *Csv) Body() ([]byte, error) {
	var err error
	r.buf, err = r.body.Csv(r.buf)
	return r.buf, err
}

func (r *Csv) Headers() (headers map[string]string) {
	return map[string]string{"content-type": "text/csv"}
}
