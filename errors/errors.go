package errors

import "net/http"

type Internal string

func NewInternal(err string) Internal {
	return Internal(err)
}

func (i Internal) Code() int {
	return http.StatusInternalServerError
}

func (i Internal) Error() string {
	return string(i)
}

type BadRequest string

func NewBadRequest(err string) BadRequest {
	return BadRequest(err)
}

func (b BadRequest) Code() int {
	return http.StatusBadRequest
}

func (b BadRequest) Error() string {
	return string(b)
}
