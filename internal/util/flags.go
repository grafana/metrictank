package util

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// Registerer is a thing that can RegisterFlags
type Registerer interface {
	RegisterFlags(*flag.FlagSet)
}

// RegisterFlags registers flags with the provided Registerers
func RegisterFlags(rs ...Registerer) {
	for _, r := range rs {
		r.RegisterFlags(flag.CommandLine)
	}
}

type Int64SliceFlag []int64

func (i *Int64SliceFlag) Set(value string) error {
	for _, split := range strings.Split(value, ",") {
		split = strings.TrimSpace(split)
		if split == "" {
			continue
		}
		parsed, err := strconv.Atoi(split)
		if err != nil {
			return err
		}
		*i = append(*i, int64(parsed))
	}
	return nil
}

func (i *Int64SliceFlag) String() string {
	// This is just a 1-liner to print a slice as a comma separated list.
	return strings.Trim(strings.Replace(fmt.Sprint(*i), " ", ", ", -1), "[]")
}
