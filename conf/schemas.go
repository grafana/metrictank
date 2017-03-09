package conf

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/alyu/configparser"
)

var DefaultSchema = Schema{
	Name:       "default",
	Pattern:    nil,
	Retentions: Retentions([]Retention{NewRetentionMT(60, 3600*24*7, 120*60, 2, true)}),
}

// Schemas contains schema settings
type Schemas []Schema

func (s Schemas) Len() int           { return len(s) }
func (s Schemas) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Schemas) Less(i, j int) bool { return s[i].Priority >= s[j].Priority }

// Schema represents one schema setting
type Schema struct {
	Name       string
	Pattern    *regexp.Regexp
	Retentions Retentions
	Priority   int64
}

func NewSchemas() []Schema {
	return nil
}

// ReadSchemas reads and parses a storage-schemas.conf file and returns a sorted schemas structure
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
func ReadSchemas(file string) (Schemas, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return nil, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	var schemas Schemas

	for i, sec := range sections {
		schema := Schema{}
		schema.Name = strings.Trim(strings.SplitN(sec.String(), "\n", 2)[0], " []")
		if schema.Name == "" || strings.HasPrefix(schema.Name, "#") {
			continue
		}

		if sec.ValueOf("pattern") == "" {
			return nil, fmt.Errorf("[%s]: empty pattern", schema.Name)
		}
		schema.Pattern, err = regexp.Compile(sec.ValueOf("pattern"))
		if err != nil {
			return nil, fmt.Errorf("[%s]: failed to parse pattern %q: %s", schema.Name, sec.ValueOf("pattern"), err.Error())
		}

		schema.Retentions, err = ParseRetentions(sec.ValueOf("retentions"))
		if err != nil {
			return nil, fmt.Errorf("[%s]: failed to parse retentions %q: %s", schema.Name, sec.ValueOf("retentions"), err.Error())
		}

		p := int64(0)
		if sec.ValueOf("priority") != "" {
			p, err = strconv.ParseInt(sec.ValueOf("priority"), 10, 0)
			if err != nil {
				return nil, fmt.Errorf("Failed to parse priority %q for [%s]: %s", sec.ValueOf("priority"), schema.Name, err)
			}
		}
		schema.Priority = int64(p)<<32 - int64(i) // to sort records with same priority by position in file

		schemas = append(schemas, schema)
	}

	sort.Sort(schemas)
	return schemas, nil
}

// Match returns the correct schema setting for the given metric
// it can always find a valid setting, because there's a default catch all
// also returns the index of the setting, to efficiently reference it
func (s Schemas) Match(metric string) (uint16, Schema) {
	for i, schema := range s {
		if schema.Pattern.MatchString(metric) {
			return uint16(i), schema
		}
	}
	return uint16(len(s)), DefaultSchema
}

// Get returns the schema setting corresponding to the given index
func (s Schemas) Get(i uint16) Schema {
	if i+1 > uint16(len(s)) {
		return DefaultSchema
	}
	return s[i]
}
