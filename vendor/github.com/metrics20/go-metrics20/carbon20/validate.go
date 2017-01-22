package carbon20

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var errTooManyEquals = errors.New("more than 1 equals")
var errKeyOrValEmpty = errors.New("tag_k and tag_v must be non-empty strings")
var errWrongNumFields = errors.New("packet must consist of 3 fields")
var errValNotNumber = errors.New("value field is not a float or int")
var errTsNotTs = errors.New("timestamp field is not a unix timestamp")
var errEmptyNode = errors.New("empty node")
var errMixEqualsTypes = errors.New("both = and _is_")
var errNoUnit = errors.New("no unit tag")
var errNoMType = errors.New("no mtype tag")
var errNotEnoughTags = errors.New("must have at least 1 tag beyond unit and mtype")

var errFmtNullAt = "null byte at position %d"
var errFmtIllegalChar = "illegal char %q"
var errFmtNonAsciiChar = "non-ASCII char %q"

// ValidationLevelLegacy indicates the level of validation to undertake for legacy metrics
//go:generate stringer -type=ValidationLevelLegacy
type ValidationLevelLegacy int

const (
	StrictLegacy ValidationLevelLegacy = iota // Sensible character validation and no consecutive dots
	MediumLegacy                              // Ensure characters are 8-bit clean and not NULL
	NoneLegacy                                // No validation
)

// ValidationLevelM20 indicates validation level for both M20 and M20NoEquals types
//go:generate stringer -type=ValidationLevelM20
type ValidationLevelM20 int

const (
	StrictM20 ValidationLevelM20 = iota // not implemented. reserved for if a nead appears
	MediumM20                           // unit, mtype tag set. no mixing of = and _is_ styles. at least two tags.
	NoneM20
)

// helper functions

// validateSensibleChars checks that the metric id only contains characters that
// are commonly understood to be sensible and useful.  Because Graphite will do
// the weirdest things with all kinds of special characters.
func validateSensibleChars(metric_id string) error {
	for _, ch := range metric_id {
		if !(ch >= 'a' && ch <= 'z') && !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9') && ch != '_' && ch != '-' && ch != '.' {
			return fmt.Errorf(errFmtIllegalChar, ch)
		}
	}
	return nil
}

// validateSensibleCharsB is like ValidateSensibleChars but for byte array inputs.
func validateSensibleCharsB(metric_id []byte) error {
	for _, ch := range metric_id {
		if !(ch >= 'a' && ch <= 'z') && !(ch >= 'A' && ch <= 'Z') && !(ch >= '0' && ch <= '9') && ch != '_' && ch != '-' && ch != '.' {
			return fmt.Errorf(errFmtIllegalChar, ch)
		}
	}
	return nil
}

// validateNotNullAsciiChars returns true if all bytes in metric_id are 8-bit
// clean and no byte is a NULL byte. Otherwise, it returns false.
func validateNotNullAsciiChars(metric_id []byte) error {
	for i, ch := range metric_id {
		if ch == 0 {
			return fmt.Errorf(errFmtNullAt, i)
		}
		if ch&0x80 != 0 {
			return fmt.Errorf(errFmtNonAsciiChar, ch)
		}
	}
	return nil
}

// public functions

// ValidateKey checks the basic form of metric keys
func ValidateKeyLegacy(metric_id string, level ValidationLevelLegacy) error {
	if level == StrictLegacy {
		// if the metric contains no = or _is_, in theory we don't really care what it does contain.  it can be whatever.
		// in practice, graphite alters (removes a dot) the metric id when this happens:
		if strings.Contains(metric_id, "..") {
			return errEmptyNode
		}
		return validateSensibleChars(metric_id)

	} else if level == MediumLegacy {
		return validateNotNullAsciiChars([]byte(metric_id))
	}
	return nil
}
func ValidateKeyM20(metric_id string, level ValidationLevelM20) error {
	if level == NoneM20 {
		return nil
	}
	if strings.Contains(metric_id, "_is_") {
		return errMixEqualsTypes
	}
	if !strings.HasPrefix(metric_id, "unit=") && !strings.Contains(metric_id, ".unit=") {
		return errNoUnit
	}
	if !strings.HasPrefix(metric_id, "mtype=") && !strings.Contains(metric_id, ".mtype=") {
		return errNoMType
	}
	if strings.Count(metric_id, ".") < 2 {
		return errNotEnoughTags
	}
	return nil
}
func ValidateKeyM20NoEquals(metric_id string, level ValidationLevelM20) error {
	if level == NoneM20 {
		return nil
	}
	if strings.Contains(metric_id, "=") {
		return errMixEqualsTypes
	}
	if !strings.HasPrefix(metric_id, "unit_is_") && !strings.Contains(metric_id, ".unit_is_") {
		return errNoUnit
	}
	if !strings.HasPrefix(metric_id, "mtype_is_") && !strings.Contains(metric_id, ".mtype_is_") {
		return errNoMType
	}
	if strings.Count(metric_id, ".") < 2 {
		return errNotEnoughTags
	}
	return nil
}

// optimization so compiler doesn't initialize and allocate new variables every time we use this.
// shouldn't be needed for the strings above because they are immutable, I'm assuming the compiler optimizes for that
var (
	doubleDot    = []byte("..")
	m20Is        = []byte("_is_")
	m20UnitPre   = []byte("unit=")
	m20UnitMid   = []byte(".unit=")
	m20MTPre     = []byte("mtype=")
	m20MTMid     = []byte(".mtype=")
	m20NEIS      = []byte("=")
	m20NEUnitPre = []byte("unit_is_")
	m20NEUnitMid = []byte(".unit_is_")
	m20NEMTPre   = []byte("mtype_is_")
	m20NEMTMid   = []byte(".mtype_is_")
	dot          = []byte(".")
)

// ValidateKeyB is like ValidateKey but for byte array inputs.
func ValidateKeyLegacyB(metric_id []byte, level ValidationLevelLegacy) error {
	if level == StrictLegacy {
		if bytes.Contains(metric_id, doubleDot) {
			return errEmptyNode
		}
		return validateSensibleCharsB(metric_id)
	} else if level == MediumLegacy {
		return validateNotNullAsciiChars(metric_id)
	}
	return nil
}
func ValidateKeyM20B(metric_id []byte, level ValidationLevelM20) error {
	if level == NoneM20 {
		return nil
	}
	if bytes.Contains(metric_id, m20Is) {
		return errMixEqualsTypes
	}
	if !bytes.HasPrefix(metric_id, m20UnitPre) && !bytes.Contains(metric_id, m20UnitMid) {
		return errNoUnit
	}
	if !bytes.HasPrefix(metric_id, m20MTPre) && !bytes.Contains(metric_id, m20MTMid) {
		return errNoMType
	}
	if bytes.Count(metric_id, dot) < 2 {
		return errNotEnoughTags
	}
	return nil
}
func ValidateKeyM20NoEqualsB(metric_id []byte, level ValidationLevelM20) error {
	if level == NoneM20 {
		return nil
	}
	if bytes.Contains(metric_id, m20NEIS) {
		return errMixEqualsTypes
	}
	if !bytes.HasPrefix(metric_id, m20NEUnitPre) && !bytes.Contains(metric_id, m20NEUnitMid) {
		return errNoUnit
	}
	if !bytes.HasPrefix(metric_id, m20NEMTPre) && !bytes.Contains(metric_id, m20NEMTMid) {
		return errNoMType
	}
	if bytes.Count(metric_id, dot) < 2 {
		return errNotEnoughTags
	}
	return nil
}

var space = []byte(" ")
var empty = []byte("")

// ValidatePacket validates a carbon message and returns useful pieces of it
func ValidatePacket(buf []byte, levelLegacy ValidationLevelLegacy, levelM20 ValidationLevelM20) ([]byte, float64, uint32, error) {
	fields := bytes.Fields(buf)
	if len(fields) != 3 {
		return empty, 0, 0, errWrongNumFields
	}

	version := GetVersionB(fields[0])
	var err error
	if version == Legacy {
		err = ValidateKeyLegacyB(fields[0], levelLegacy)
	} else if version == M20 {
		err = ValidateKeyM20B(fields[0], levelM20)
	} else { // version == M20NoEquals
		err = ValidateKeyM20NoEqualsB(fields[0], levelM20)
	}
	if err != nil {
		return empty, 0, 0, err
	}

	val, err := strconv.ParseFloat(string(fields[1]), 32)
	if err != nil {
		return empty, 0, 0, errValNotNumber
	}

	ts, err := strconv.ParseUint(string(fields[2]), 10, 0)
	if err != nil {
		return empty, 0, 0, errTsNotTs
	}

	return fields[0], val, uint32(ts), nil
}
