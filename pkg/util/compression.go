package util

import (
	"compress/gzip"
	"io"
	"io/ioutil"
)

func DecompressGzip(r io.Reader) (string, error) {
	reader, err := gzip.NewReader(r)
	if err != nil {
		return "", err
	}

	unzipped, err := ioutil.ReadAll(reader)
	return string(unzipped), err
}
