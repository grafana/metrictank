package test

import (
	"path/filepath"
	"runtime"
)

// path takes a relative path within the metrictank repository and returns the full absolute filepath,
// assuming metrictank repo is in the first directory specified in GOPATH
func Path(dst string) string {
	_, b, _, _ := runtime.Caller(0)
	// Gets the path of this file (pkg/test/path.go), so we use ../../ to get
	// to project root.
	basepath := filepath.Dir(b)
	return filepath.Join(basepath, "../..", dst)
}
