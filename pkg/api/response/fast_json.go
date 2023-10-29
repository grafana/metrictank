package response

type FastJSON interface {
	MarshalJSONFast([]byte) ([]byte, error)
}

type FastJson struct {
	code int
	body FastJSON
	buf  []byte
}

func NewFastJson(code int, body FastJSON) *FastJson {
	return &FastJson{
		code: code,
		body: body,
		buf:  BufferPool.Get(),
	}
}

func (r *FastJson) Code() int {
	return r.code
}

func (r *FastJson) Close() {
	BufferPool.Put(r.buf)
}

func (r *FastJson) Body() ([]byte, error) {
	var err error
	r.buf, err = r.body.MarshalJSONFast(r.buf)
	return r.buf, err
}

func (r *FastJson) Headers() (headers map[string]string) {
	headers = map[string]string{"content-type": "application/json"}
	return headers
}
