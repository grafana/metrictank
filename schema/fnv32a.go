package schema

import "hash"

// sum32aStringWriter is mostly a copy of fnv.sum32a
// the only difference is the additional method WriteString(),
// due to this additional method it satisfies the io.StringWriter
// interface which can prevent unnecessary conversions to/from
// byte slices
type sum32aStringWriter uint32

const offset32 = 2166136261
const prime32 = 16777619

func fnvNew32aStringWriter() hash.Hash32 {
	var s sum32aStringWriter = offset32
	return &s
}

func (s *sum32aStringWriter) Reset()         { *s = offset32 }
func (s *sum32aStringWriter) Sum32() uint32  { return uint32(*s) }
func (s *sum32aStringWriter) BlockSize() int { return 1 }

func (s *sum32aStringWriter) Sum(in []byte) []byte {
	v := uint32(*s)
	return append(in, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func (s *sum32aStringWriter) Size() int { return 4 }

func (s *sum32aStringWriter) Write(data []byte) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= sum32aStringWriter(c)
		hash *= prime32
	}
	*s = hash
	return len(data), nil
}
func (s *sum32aStringWriter) WriteString(data string) (int, error) {
	hash := *s
	for _, c := range data {
		hash ^= sum32aStringWriter(c)
		hash *= prime32
	}
	*s = hash
	return len(data), nil
}
