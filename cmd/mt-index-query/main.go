package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

var (
	base = flag.String("base", "http://localhost:6060", "base url")
	mkey = flag.String("mkey", "", "the mkey to query")
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	var err error
	flag.Parse()
	if *mkey == "" {
		// TODO Validation
		log.Fatal("mkey must be specified")
	}

	resp, err := http.PostForm(*base+"/index/get", url.Values{"MKey": {*mkey}})
	if err != nil {
		log.Fatal(err.Error())
	}
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("response status %d - body: %s", resp.StatusCode, string(b))
	}
	var a idx.Archive
	read := msgp.NewReader(resp.Body)
	err = a.DecodeMsg(read)
	if err != nil {
		log.Fatal(err.Error())
	}
	spew.Dump(a)
}
