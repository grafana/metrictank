package schema

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

//go:generate msgp
//msgp:ignore AMKey
// don't ignore Key, MKey because it's used for MetricDefinition

var ErrStringTooShort = errors.New("string too short")
var ErrInvalidFormat = errors.New("invalid format")

// Key identifies a metric
type Key [16]byte

// MKey uniquely identifies a metric in a multi-tenant context
type MKey struct {
	Key Key
	Org uint32
}

// KeyFromString parses a string id to an MKey
// string id must be of form orgid.<hexadecimal 128bit hash>
func MKeyFromString(s string) (MKey, error) {
	l := len(s)

	// shortest an orgid can be is single digit
	if l < 34 {
		return MKey{}, ErrStringTooShort
	}

	hashStr := s[l-32:]
	orgStr := s[0 : l-33]

	hash, err := hex.DecodeString(hashStr)
	if err != nil {
		return MKey{}, err
	}

	org, err := strconv.ParseUint(orgStr, 10, 32)
	if err != nil {
		return MKey{}, err
	}

	k := MKey{
		Org: uint32(org),
	}

	copy(k.Key[:], hash)
	return k, nil
}

func (m MKey) String() string {
	return fmt.Sprintf("%d.%x", m.Org, m.Key)
}

// AMKey is a multi-tenant key with archive extension
// so you can refer to rollup archives
type AMKey struct {
	MKey    MKey
	Archive Archive
}

func (a AMKey) String() string {
	if a.Archive == 0 {
		return a.MKey.String()
	}
	return a.MKey.String() + "_" + a.Archive.String()
}

// GetAMKey helps to easily get an AMKey from a given MKey
func GetAMKey(m MKey, method Method, span uint32) AMKey {
	return AMKey{
		MKey:    m,
		Archive: NewArchive(method, span),
	}
}

func AMKeyFromString(s string) (AMKey, error) {
	underscores := strings.Count(s, "_")
	amk := AMKey{}
	var err error
	switch underscores {
	case 0:
		amk.MKey, err = MKeyFromString(s)
		return amk, err
	case 2:
		pos := strings.Index(s, "_")
		amk.MKey, err = MKeyFromString(s[:pos])
		if err != nil {
			return amk, err
		}
		amk.Archive, err = ArchiveFromString(s[pos+1:])
		return amk, err

	}
	return amk, ErrInvalidFormat
}
