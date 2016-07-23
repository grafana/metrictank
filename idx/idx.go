// The idx package provides a metadata index for metrics
// it's based on the index from https://github.com/dgryski/carbonmem/
// it seems promising and performant for our current needs, but also experimental
// We don't necessarily want to start maintaining our own little search engine.
// Future needs (scale, features) may deprecate this package in favor of something else
// like ES, bleve, etc.  But for now it seems to work well, so let's see where it takes us
//
// callers are responsible for thread safety, period pruning if desired
package idx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/armon/go-radix"
	"github.com/dgryski/go-trigram"
	"gopkg.in/raintank/schema.v1"
)

type MatchType uint8

const (
	MatchLiteral MatchType = iota
	MatchPrefix
	MatchTrigram
)

type DocID uint32     // same thing as trigram.DocID. just syntactic sugar
type MetricID string  // our metric id's like 123.deadbeefc0xed
type MetricKey string // graphite path.like.so

type Glob struct {
	Metric MetricKey
	IsLeaf bool
}

func (g Glob) String() string {
	return fmt.Sprintf("Glob metric=%s leaf=%t", g.Metric, g.IsLeaf)
}

const OTTBase = uint32(2147483648)             // 1 << 31
const PublicOrgTrigram = trigram.T(2147483648) // 1 << 31. eg base + 0

// orgToTrigram takes in an orgid between -1 (special) and 214748345 and converts it to a special trigram
func orgToTrigram(org int) trigram.T {
	if org < -1 {
		panic("orgid must be >= -1")
	}
	// org+1 cannot be 2147483647 because we would end up at MaxUint32 == 4294967295 == 0xffffffff which is reserved
	if org > 2147483645 {
		panic("org too high")
	}
	ret := OTTBase + uint32(org) + 1
	return trigram.T(ret)
}

// Idx provides fast access to metricdefinitions based on several inputs:
// * MetricID 1:1 look up
// * graphite.key or graphite.* prefix lookup using a radix tree (1:M)
// * *.foo.{bar,baz}.ba* style lookup using a trigram index (1:M)
// internally we track definitions by DocID and the various indices resolve to DocID's.
// note that MetricID's and DocID's are globally unique, across orgs
// metric names are not, and we have to deal with that to provide multi-tenancy.
type Idx struct {
	defs  []schema.MetricDefinition // docID to metric definition.
	count int                       // global across all orgs

	byid   map[MetricID]DocID // metric id to DocID
	prefix []*radix.Tree      // metric name prefix to DocID. per org. each org is stored at pos orgid+1 because we have a special org -1
	tridx  trigram.Index      // metric name snippets to DocID. uses special org-id trigrams to separate by org
}

func New() *Idx {
	return &Idx{
		defs: make([]schema.MetricDefinition, 0),

		byid:   make(map[MetricID]DocID),
		prefix: make([]*radix.Tree, 0),
		tridx:  trigram.NewIndex(nil),
	}

}

func (l *Idx) Len() int {
	return len(l.byid)
}

// Add adds metricdefinitions to the index.  It's the callers
// responsibility to make sure that no def with the same .Id field exists already
func (l *Idx) Add(def schema.MetricDefinition) DocID {
	id := DocID(l.count)
	l.count++

	l.defs = append(l.defs, def)

	l.byid[MetricID(def.Id)] = id

	// like l.tridx.Insert(key, trigram.DocID(id)) but with added org trigram
	ts := trigram.ExtractAll(def.Name, nil)
	ts = append(ts, orgToTrigram(def.OrgId))
	l.tridx.InsertTrigrams(ts, trigram.DocID(id))

	pos := def.OrgId + 1
	for len(l.prefix) < pos+1 {
		l.prefix = append(l.prefix, radix.New())
	}
	l.prefix[pos].Insert(def.Name, id)

	return id
}

func (l *Idx) Update(def schema.MetricDefinition) {
	id := l.byid[MetricID(def.Id)]
	l.defs[id] = def
}

func (l *Idx) GetById(metricID MetricID) *schema.MetricDefinition {
	d, ok := l.byid[metricID]
	if !ok {
		return nil
	}
	return &l.defs[d]
}

func (l *Idx) WalkPrefix(org int, query string, fn radix.WalkFn) {
	pos := org + 1
	for len(l.prefix) < pos+1 {
		l.prefix = append(l.prefix, radix.New())
	}
	l.prefix[pos].WalkPrefix(query, fn)
}

func (l *Idx) Walk(org int, fn radix.WalkFn) {
	pos := org + 1
	for len(l.prefix) < pos+1 {
		l.prefix = append(l.prefix, radix.New())
	}
	l.prefix[pos].Walk(fn)
}

// for a "filesystem glob" query (e.g. supports * [] and {})
// looks in the trigrams index
func (l *Idx) QueryTrigrams(org int, query string) ([]Glob, []*schema.MetricDefinition) {

	if l.tridx == nil {
		return nil, nil
	}

	ts := extractTrigrams(query)
	ids := l.tridx.QueryTrigrams(ts)
	ids = l.tridx.FilterOr(ids, [][]trigram.T{{PublicOrgTrigram}, {orgToTrigram(org)}})

	queries := expandQueries(query)

	seen := make(map[string]bool)
	globs := make([]Glob, 0)
	defs := make([]*schema.MetricDefinition, 0)

	for _, id := range ids {

		name := l.defs[id].Name
		dir := filepath.Dir(name)

		// if dir foo.bar already matched the expr, then any child foo.bar.baz won't match
		// because all patterns are formed such that each level is explicit.
		if seen[dir] {
			continue
		}

		if matchAny(queries, dir) {
			seen[dir] = true
			// directories are a special case, they don't correspond to an actual metricdefinition
			globs = append(globs, Glob{MetricKey(dir), false})
			defs = append(defs, nil)
			continue
		}

		if matchAny(queries, name) {
			seen[name] = true
			globs = append(globs, Glob{MetricKey(name), true})
			defs = append(defs, &l.defs[id])
		}
	}
	return globs, defs
}

func matchAny(queries []string, id string) bool {
	for _, query := range queries {
		if matched, err := filepath.Match(query, id); err == nil && matched {
			return true
		}
	}
	return false
}

// filepath.Match doesn't support {} because that's not posix, it's a bashism
// the easiest way of implementing this extra feature is just expanding single queries
// that contain these queries into multiple queries, who will be checked separately
// and whose results will be ORed.
func expandQueries(query string) []string {
	queries := []string{query}

	// as long as we find a { followed by a }, split it up into subqueries, and process
	// all queries again
	// we only stop once there are no more queries that still have {..} in them
	keepLooking := true
	for keepLooking {
		expanded := make([]string, 0)
		keepLooking = false
		for _, query := range queries {
			lbrace := strings.Index(query, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(query[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				keepLooking = true
				expansion := query[lbrace+1 : rbrace]
				options := strings.Split(expansion, ",")
				for _, option := range options {
					expanded = append(expanded, query[:lbrace]+option+query[rbrace+1:])
				}
			} else {
				expanded = append(expanded, query)
			}
		}
		queries = expanded
	}
	return queries
}

func (l *Idx) List(org int) []*schema.MetricDefinition {
	var defs []*schema.MetricDefinition
	fn := func(k string, v interface{}) bool {
		defs = append(defs, &l.defs[int(v.(DocID))])
		return false
	}
	if org == -1 {
		for i := 0; i < len(l.prefix); i++ {
			l.prefix[i].Walk(fn)
		}
	} else {
		l.Walk(-1, fn)
		l.Walk(org, fn)
	}
	return defs
}

// can do equality checks as well as prefix checks
// for a given query 'foo.b'
// results like:
// foo.bar -> leaf (a valid metric)
// foo.bar.baz -> not a leaf: return the "directory" foo.bar
// in equality mode, will only return foo.b, not foo.bar
func (i *Idx) QueryRadix(org int, query string, equal bool) ([]Glob, []*schema.MetricDefinition) {
	var globs []Glob
	var defs []*schema.MetricDefinition
	l := len(query)
	seen := make(map[string]bool)
	fn := func(k string, v interface{}) bool {
		// figure out if we're a leaf or not
		dot := strings.IndexByte(k[l:], '.')
		var leaf bool
		m := k
		if dot == -1 {
			leaf = true
		} else {
			m = k[:dot+l]
		}

		if len(m) > len(query) && equal {
			return false
		}
		if !seen[m] {
			seen[m] = true
			globs = append(globs, Glob{Metric: MetricKey(m), IsLeaf: leaf})
			defs = append(defs, &i.defs[int(v.(DocID))])
		}
		// false == "don't terminate iteration"
		return false
	}
	i.WalkPrefix(-1, query, fn)
	i.WalkPrefix(org, query, fn)
	return globs, defs
}

// output is unsorted, if you want it sorted, do it yourself
func (i *Idx) Match(org int, query string) (MatchType, []Glob, []*schema.MetricDefinition) {

	// exact match only, could still result in multiple definitions.
	// the radix tree is a good option for this case
	if !strings.ContainsAny(query, "*{}[]?") {
		globs, defs := i.QueryRadix(org, query, true)
		return MatchLiteral, globs, defs
	}

	// only one trailing star -> prefix match
	if strings.Index(query, "*") == len(query)-1 && !strings.ContainsAny(query, "{}[]?") {
		query = query[:len(query)-1]
		globs, defs := i.QueryRadix(org, query, false)
		return MatchPrefix, globs, defs
	}

	// at least one interior star or a more complicated query
	globs, defs := i.QueryTrigrams(org, query)
	return MatchTrigram, globs, defs
}

func extractTrigrams(query string) []trigram.T {

	if len(query) < 3 {
		return nil
	}

	var start int
	var i int

	var trigrams []trigram.T

	for i < len(query) {
		if query[i] == '[' || query[i] == '{' || query[i] == '*' || query[i] == '?' {
			trigrams = trigram.Extract(query[start:i], trigrams)

			if query[i] == '[' {
				for i < len(query) && query[i] != ']' {
					i++
				}
			}

			if query[i] == '{' {
				for i < len(query) && query[i] != '}' {
					i++
				}
			}

			start = i + 1
		}
		i++
	}

	trigrams = trigram.Extract(query[start:i], trigrams)

	return trigrams
}

// Prune is a custom pruning function for the trigram index
// it removes all trigrams that are present in more than the specified percentage of the documents.
// However, it makes sure that neither tAllDocIDs,
// nor any of the org specific trigrams used for access control are ever pruned.
func (i *Idx) Prune(pct float64) {
	maxDocs := int(pct * float64(len(i.tridx[trigram.TAllDocIDs])))
	for k, v := range i.tridx {
		// all values >= 1<<31 are special trigrams that should be kept
		if uint32(k) < uint32(2147483648) && len(v) > maxDocs {
			i.tridx[k] = nil
		}
	}
}
