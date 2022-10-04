package middleware

import (
	"bytes"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func CaptureBody(c *Context) {
	body, err := ioutil.ReadAll(c.Req.Request.Body)
	if err != nil {
		log.Errorf("HTTP internal error: failed to read request body for proxying: %s", err.Error())
		c.PlainText(http.StatusInternalServerError, []byte("internal error: failed to read request body for proxying"))
	}
	c.Req.Request.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	c.Body = ioutil.NopCloser(bytes.NewBuffer(body))
}
