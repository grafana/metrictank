package util

import "hash"

type StringHash64 interface {
	hash.Hash64
	WriteString(string) (int, error)
}

// Sum64aStringWriter is mostly a copy of fnv.sum64a
// the only difference is the additional method WriteString(),
// due to this additional method it satisfies the io.StringWriter
// interface which can prevent unnecessary conversions to/from
// byte slices
type Sum64aStringWriter uint64

const offset64 = 14695981039346656037
const prime64 = 1099511628211

func NewFnv64aStringWriter() StringHash64 {
	var s Sum64aStringWriter = offset64
	return &s
}

func (s *Sum64aStringWriter) BlockSize() int { return 1 }

func (s *Sum64aStringWriter) Reset() { *s = offset64 }

func (s *Sum64aStringWriter) Size() int { return 8 }

func (s *Sum64aStringWriter) Sum(in []byte) []byte {
	v := uint64(*s)
	return append(in, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (s *Sum64aStringWriter) Sum64() uint64 { return uint64(*s) }

func (s *Sum64aStringWriter) Write(data []byte) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= Sum64aStringWriter(c)
		hash *= prime64
	}
	*s = hash
	return len(data), nil
}

func (s *Sum64aStringWriter) WriteString(data string) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= Sum64aStringWriter(c)
		hash *= prime64
	}
	*s = hash
	return len(data), nil
}
