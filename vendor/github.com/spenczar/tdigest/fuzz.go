// +build gofuzz

package tdigest

import (
	"bytes"
	"fmt"
	"log"

	"github.com/davecgh/go-spew/spew"
)

func Fuzz(data []byte) int {
	v := new(TDigest)
	err := v.UnmarshalBinary(data)
	if err != nil {
		return 0
	}

	remarshaled, err := v.MarshalBinary()
	if err != nil {
		panic(err)
	}

	if !bytes.HasPrefix(data, remarshaled) {
		panic(fmt.Sprintf("not equal: \n%v\nvs\n%v", data, remarshaled))
	}

	for q := float64(0.1); q <= 1.0; q += 0.05 {
		prev, this := v.Quantile(q-0.1), v.Quantile(q)
		if prev-this > 1e-100 { // Floating point math makes this slightly imprecise.
			log.Printf("v: %s", spew.Sprint(v))
			log.Printf("q: %v", q)
			log.Printf("prev: %v", prev)
			log.Printf("this: %v", this)
			panic("quantiles should only increase")
		}
	}

	v.Add(1, 1)
	return 1
}
