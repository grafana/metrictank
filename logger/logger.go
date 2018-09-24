// Package logger has a custom TextFormatter for use with logrus
package logger

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const defaultTimestampFormat = time.RFC3339

// TextFormatter formats logs into text
type TextFormatter struct {
	// Disable timestamp logging. useful when output is redirected to logging
	// system that already adds timestamps
	DisableTimestamp bool

	// Disable the conversion of the log levels to uppercase
	DisableUppercase bool

	// Timestamp format to use for display when a full timestamp is printed
	TimestampFormat string

	// The fields are sorted by default for a consistent output
	DisableSorting bool

	// Wrap empty fields in quotes if true
	QuoteEmptyFields bool

	// Can be set to the override the default quoting character "
	// with something else. For example: ', or `.
	QuoteCharacter string

	// The name of the module (webserver-5, redis-2, cluster-kafka-2, etc...)
	// prints before the log message
	ModuleName string

	sync.Once
}

func (f *TextFormatter) init(entry *logrus.Entry) {
	if len(f.QuoteCharacter) == 0 {
		f.QuoteCharacter = "\""
	}
}

// Format renders a single log entry
func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer

	keys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		keys = append(keys, k)
	}
	lastKeyIdx := len(keys) - 1

	if !f.DisableSorting {
		sort.Strings(keys)
	}

	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	timestampFormat := f.TimestampFormat
	if timestampFormat == "" {
		timestampFormat = defaultTimestampFormat
	}

	f.Do(func() { f.init(entry) })

	if !f.DisableTimestamp {
		b.WriteString(entry.Time.Format(timestampFormat))
		b.WriteByte(' ')
	}

	if !f.DisableUppercase {
		b.WriteString(strings.ToUpper(entry.Level.String()))
	} else {
		b.WriteString(entry.Level.String())
	}
	b.WriteByte(' ')

	if f.ModuleName != "" {
		b.WriteString(f.ModuleName)
		b.WriteByte(' ')
	}

	if entry.Message != "" {
		b.WriteString(entry.Message)
		if lastKeyIdx >= 0 {
			b.WriteByte(' ')
		}
	}

	for i, key := range keys {
		f.appendKeyValue(b, key, entry.Data[key], lastKeyIdx != i)
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func (f *TextFormatter) needsQuoting(text string) bool {
	if f.QuoteEmptyFields && len(text) == 0 {
		return true
	}
	for _, ch := range text {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.') {
			return true
		}
	}
	return false
}

func (f *TextFormatter) appendKeyValue(b *bytes.Buffer, key string, value interface{}, appendSpace bool) {
	b.WriteString(key)
	b.WriteByte('=')
	f.appendValue(b, value)

	if appendSpace {
		b.WriteByte(' ')
	}
}

func (f *TextFormatter) appendValue(b *bytes.Buffer, value interface{}) {
	switch value := value.(type) {
	case string:
		if !f.needsQuoting(value) {
			b.WriteString(value)
		} else {
			fmt.Fprintf(b, "%s%v%s", f.QuoteCharacter, value, f.QuoteCharacter)
		}
	case error:
		errmsg := value.Error()
		if !f.needsQuoting(errmsg) {
			b.WriteString(errmsg)
		} else {
			fmt.Fprintf(b, "%s%v%s", f.QuoteCharacter, errmsg, f.QuoteCharacter)
		}
	default:
		fmt.Fprint(b, value)
	}
}
