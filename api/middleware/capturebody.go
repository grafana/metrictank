package middleware

import (
	"bytes"
	"io/ioutil"
)

func CaptureBody(c *Context) {
	body, _ := ioutil.ReadAll(c.Req.Request.Body)
	c.Req.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	c.Body = ioutil.NopCloser(bytes.NewBuffer(body))
}
