package errors

import (
	"fmt"
	"net/http"
)

type Internal string

func NewInternal(err string) Internal {
	return Internal(err)
}

func NewInternalf(format string, a ...interface{}) Internal {
	return Internal(fmt.Sprintf(format, a...))
}

func (i Internal) HTTPStatusCode() int {
	return http.StatusInternalServerError
}

func (i Internal) Error() string {
	return string(i)
}

type BadRequest string

func NewBadRequest(err string) BadRequest {
	return BadRequest(err)
}

func NewBadRequestf(format string, a ...interface{}) BadRequest {
	return BadRequest(fmt.Sprintf(format, a...))
}

func (b BadRequest) HTTPStatusCode() int {
	return http.StatusBadRequest
}

func (b BadRequest) Error() string {
	return string(b)
}
