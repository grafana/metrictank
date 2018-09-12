package middleware

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/raintank/worldping-api/pkg/log"
)

func CaptureBody(c *Context) {
	body, err := ioutil.ReadAll(c.Req.Request.Body)
	if err != nil {
		log.Error(3, "HTTP internal error: failed to read request body for proxying: %s", err)
		c.PlainText(http.StatusInternalServerError, []byte("internal error: failed to read request body for proxying"))
	}
	c.Req.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	c.Body = ioutil.NopCloser(bytes.NewBuffer(body))
}
