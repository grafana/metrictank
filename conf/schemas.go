package conf

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/alyu/configparser"
	"github.com/raintank/metrictank/util"
)

// Schemas contains schema settings
type Schemas struct {
	raw           []Schema
	index         []Schema
	DefaultSchema Schema
}

type SchemaSlice []Schema

func (s SchemaSlice) Len() int           { return len(s) }
func (s SchemaSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SchemaSlice) Less(i, j int) bool { return s[i].Priority >= s[j].Priority }

// Schema represents one schema setting
type Schema struct {
	Name       string
	Pattern    *regexp.Regexp
	Retentions Retentions
	Priority   int64
}

func NewSchemas(schemas []Schema) Schemas {
	s := Schemas{
		raw: schemas,
		DefaultSchema: Schema{
			Name:       "default",
			Pattern:    regexp.MustCompile(".*"),
			Retentions: Retentions([]Retention{NewRetentionMT(1, 3600*24*1, 600, 2, true)}), // 1s:1day:10mim:2:true
		},
	}
	s.BuildIndex()
	return s
}

func (s Schemas) List() []Schema {
	return s.raw
}

func (s *Schemas) BuildIndex() {
	s.index = make([]Schema, 0)
	for _, schema := range s.raw {
		for pos := range schema.Retentions {
			s.index = append(s.index, Schema{
				Name:       schema.Name,
				Pattern:    schema.Pattern,
				Retentions: schema.Retentions[pos:],
				Priority:   schema.Priority,
			})
		}
	}
	// add the default schema
	for pos := range s.DefaultSchema.Retentions {
		s.index = append(s.index, Schema{
			Name:       s.DefaultSchema.Name,
			Pattern:    s.DefaultSchema.Pattern,
			Retentions: s.DefaultSchema.Retentions[pos:],
			Priority:   s.DefaultSchema.Priority,
		})
	}
}

// ReadSchemas reads and parses a storage-schemas.conf file and returns a sorted schemas structure
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
func ReadSchemas(file string) (Schemas, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return Schemas{}, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return Schemas{}, err
	}

	var schemas []Schema

	for i, sec := range sections {
		schema := Schema{}
		schema.Name = strings.Trim(strings.SplitN(sec.String(), "\n", 2)[0], " []")
		if schema.Name == "" || strings.HasPrefix(schema.Name, "#") {
			continue
		}

		if sec.ValueOf("pattern") == "" {
			return Schemas{}, fmt.Errorf("[%s]: empty pattern", schema.Name)
		}
		schema.Pattern, err = regexp.Compile(sec.ValueOf("pattern"))
		if err != nil {
			return Schemas{}, fmt.Errorf("[%s]: failed to parse pattern %q: %s", schema.Name, sec.ValueOf("pattern"), err.Error())
		}

		schema.Retentions, err = ParseRetentions(sec.ValueOf("retentions"))
		if err != nil {
			return Schemas{}, fmt.Errorf("[%s]: failed to parse retentions %q: %s", schema.Name, sec.ValueOf("retentions"), err.Error())
		}

		p := int64(0)
		if sec.ValueOf("priority") != "" {
			p, err = strconv.ParseInt(sec.ValueOf("priority"), 10, 0)
			if err != nil {
				return Schemas{}, fmt.Errorf("Failed to parse priority %q for [%s]: %s", sec.ValueOf("priority"), schema.Name, err)
			}
		}
		schema.Priority = int64(p)<<32 - int64(i) // to sort records with same priority by position in file

		schemas = append(schemas, schema)
	}

	sort.Sort(SchemaSlice(schemas))

	return NewSchemas(schemas), nil
}

// Match returns the correct schema setting for the given metric
// it can always find a valid setting, because there's a default catch all
// also returns the index of the setting, to efficiently reference it.
//
// A schema is just a pattern + retention policy. A retention policy is
// just a list of retentions. The s.index slice contains a schema for each
// sublist of an original schemas retentions. So if an original
// schema had a retention policy of 1s:1d,1m:7d,1h:1y then 3 schemas would
// be added to the index with same pattern as the original but retention policies
// of "1s:1d,1m:7d,1h:1y", "1m:7d,1h:1y" and "1h:1y".
//
// |---------------------------------------------------------------------|
// |     pattern 1     |        pattern 2            |    pattern 3      |
// |---------------------------------------------------------------------|
// |   ret0  |  ret1   |  ret0   |  ret1   |  ret2   |  ret0   |  ret1   |
// |---------------------------------------------------------------------|
// | schema0 | schema1 | schema2 | schema3 | schema4 | schema5 | schema6 |
// |---------------------------------------------------------------------|
//
// When evaluating a match we start with the first schema in the index and
// compare the regex pattern.
// - If it matches we then just find the retention set with the best fit. The
//   best fit is when the interval is >= the rawInterval (first retention) and
//   less then the interval of the next rollup.
// - If the pattern doesnt match, then we skip ahead to the next pattern.
//
// eg. from the above diagram we would compare the pattern for schame0
//     (pattern1), if it doesnt match we will then compare the pattern of
//     schema2 (pattern2) and if that doesnt match we would try schema5
//     (pattern3).
func (s Schemas) Match(metric string, interval int) (uint16, Schema) {
	i := 0
	for i < len(s.index) {
		schema := s.index[i]
		if schema.Pattern.MatchString(metric) {
			// no interval passed,use the raw retentions.
			// This is primarily used by the carbon input plugin.
			if interval == 0 {
				return uint16(i), schema
			}
			// search through the retentions to find the first one where
			// the metric interval is < SecondsPerPoint of the retention.
			// The schema is then the previous retention.
			for j, ret := range schema.Retentions {
				if interval < ret.SecondsPerPoint {
					// if there are no retentions with SecondsPerPoint <= interval (j==0)
					// then we need to use the first retention. Otherwise, the retention
					// we want to use is the previous one.
					if j > 0 {
						j--
					}
					// the position in the index (schemaId) is the position of the schema we used for the
					// regex match + the position of the retention.
					pos := i + j
					return uint16(pos), s.index[pos]
				}
			}
			// no retentions found with SecondsPerPoint > interval. So lets just use the retention
			// with the largest secondsPerPoint.
			pos := i + len(schema.Retentions) - 1
			return uint16(pos), s.index[pos]
		}
		// the next len(schema.Retentions) schemas in the index all have the
		// same schema pattern, so we can skip over them.
		i = i + len(schema.Retentions)
	}

	// as the DefaultSchema is in the schemas.index, this should typically never be reached.
	// Though as the user can modify schemas.DefaultSchema we keep this for safety.
	return uint16(len(s.index)), s.DefaultSchema
}

// Get returns the schema setting corresponding to the given index
func (s Schemas) Get(i uint16) Schema {
	if i+1 > uint16(len(s.index)) {
		return s.DefaultSchema
	}
	return s.index[i]
}

// TTLs returns a slice of all TTL's seen amongst all archives of all schemas
func (schemas Schemas) TTLs() []uint32 {
	ttls := make(map[uint32]struct{})
	for _, s := range schemas.raw {
		for _, r := range s.Retentions {
			ttls[uint32(r.MaxRetention())] = struct{}{}
		}
	}
	for _, r := range schemas.DefaultSchema.Retentions {
		ttls[uint32(r.MaxRetention())] = struct{}{}
	}
	var ttlSlice []uint32
	for ttl := range ttls {
		ttlSlice = append(ttlSlice, ttl)
	}
	return ttlSlice
}

// MaxChunkSpan returns the largest chunkspan seen amongst all archives of all schemas
func (schemas Schemas) MaxChunkSpan() uint32 {
	max := uint32(0)
	for _, s := range schemas.raw {
		for _, r := range s.Retentions {
			max = util.Max(max, r.ChunkSpan)
		}
	}
	for _, r := range schemas.DefaultSchema.Retentions {
		max = util.Max(max, r.ChunkSpan)
	}
	return max
}
