package docker

import (
	"os"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
	p "path"
)

// path takes a relative path within the metrictank repository and returns the full absolute filepath,
// assuming metrictank repo is in the first directory specified in GOPATH
func Path(dst string) string {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		var err error
		gopath, err = homedir.Expand("~/go")
		if err != nil {
			panic(err)
		}
	}
	firstPath := strings.Split(gopath, ":")[0]
	return p.Join(firstPath, "src/github.com/grafana/metrictank", dst)
}
