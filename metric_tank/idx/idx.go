// The idx package provides a metadata index for metrics
// it's based on the index from https://github.com/dgryski/carbonmem/
// it seems promising and performant for our current needs, but also experimental
// We don't necessarily want to start maintaining our own little search engine.
// Future needs (scale, features) may deprecate this package in favor of something else
// like ES, bleve, etc.  But for now it seems to work well, so let's see where it takes us

// callers are responsible for thread safety, period pruning if desired
package idx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/armon/go-radix"
	"github.com/dgryski/go-trigram"
)

type MatchType uint8

const (
	MatchLiteral MatchType = iota
	MatchPrefix
	MatchTrigram
)

type MetricID uint32 // same thing as trigram.DocID. just syntactic sugar

type Glob struct {
	Id     MetricID
	Metric string
	IsLeaf bool
}

func (g Glob) String() string {
	return fmt.Sprintf("Glob id=%d metric=%s leaf=%t", g.Id, g.Metric, g.IsLeaf)
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

type Idx struct {
	// all metrics
	keys  []map[string]MetricID // slice is per org
	revs  []string              // MetricID to string key. doesn't have to be isolated per org
	count int                   // global across all orgs

	// currently 'active'
	active map[MetricID]int // doesn't have to be isolated per org

	prefix []*radix.Tree // per org. each org is stored at pos orgid+1 because we have a special org -1

	// stores all orgs in one index, but with special org-id trigrams
	Pathidx trigram.Index
}

func New() *Idx {
	return &Idx{
		keys: make([]map[string]MetricID, 0),

		active: make(map[MetricID]int),
		prefix: make([]*radix.Tree, 0),

		Pathidx: trigram.NewIndex(nil),
	}

}

func (l *Idx) Len() int {
	return len(l.keys)
}

// Get retrieves the metricId and whether it was found
// it first checks the given org, but also falls back to org -1
func (l *Idx) Get(org int, key string) (MetricID, bool) {
	pos := org + 1 // org -1 is pos 0, etc

	for len(l.keys) < pos+1 {
		l.keys = append(l.keys, make(map[string]MetricID))
	}

	id, ok := l.keys[pos][key]
	if !ok {
		id, ok = l.keys[0][key]
	}

	return id, ok
}

// GetsOrAdd retrieves the key if it exists for the org (but doesn't do additional check for org -1)
// and adds it for the org if non-existant
func (l *Idx) GetOrAdd(org int, key string) MetricID {
	pos := org + 1 // org -1 is pos 0, etc

	for len(l.keys) < pos+1 {
		l.keys = append(l.keys, make(map[string]MetricID))
	}

	id, ok := l.keys[pos][key]

	if ok {
		return id
	}

	id = MetricID(l.count)
	l.count++
	l.revs = append(l.revs, key)

	l.keys[pos][key] = id

	// like l.Pathidx.Insert(key, trigram.DocID(id)) but with added org trigram
	ts := trigram.ExtractAll(key, nil)
	ts = append(ts, orgToTrigram(org))
	l.Pathidx.InsertTrigrams(ts, trigram.DocID(id))

	return id
}

func (l *Idx) Key(id MetricID) string {
	return l.revs[id]
}

func (l *Idx) AddRef(org int, id MetricID) {
	v, ok := l.active[id]
	if !ok {
		pos := org + 1
		for len(l.prefix) < pos+1 {
			l.prefix = append(l.prefix, radix.New())
		}
		l.prefix[pos].Insert(l.revs[id], id)
	}

	l.active[id] = v + 1
}

// only call this after having made sure the org entries exists! e.g. after Get/GetOrAdd and AddRef
func (l *Idx) DelRef(org int, id MetricID) {
	l.active[id]--
	if l.active[id] == 0 {
		delete(l.active, id)
		delete(l.keys[org], l.revs[id])
		l.prefix[org+1].Delete(l.revs[id])
		if l.Pathidx != nil {
			l.Pathidx.Delete(l.revs[id], trigram.DocID(id))
		}
		l.revs[id] = ""
	}
}

func (l *Idx) Active(id MetricID) bool {
	return l.active[id] != 0
}

func (l *Idx) Prefix(org int, query string, fn radix.WalkFn) {
	pos := org + 1
	for len(l.prefix) < pos+1 {
		l.prefix = append(l.prefix, radix.New())
	}
	l.prefix[pos].WalkPrefix(query, fn)
}

// for a "filesystem glob" query (e.g. supports * and [] but not {})
// looks in the trigrams index
func (l *Idx) QueryPath(org int, query string) []Glob {

	if l.Pathidx == nil {
		return nil
	}

	ts := extractTrigrams(query)
	ids := l.Pathidx.QueryTrigrams(ts)
	ids = l.Pathidx.FilterOr(ids, [][]trigram.T{{PublicOrgTrigram}, {orgToTrigram(org)}})

	seen := make(map[string]bool)
	out := make([]Glob, 0)

	for _, id := range ids {

		p := l.revs[MetricID(id)]

		dir := filepath.Dir(p)

		if seen[dir] {
			continue
		}

		if matched, err := filepath.Match(query, dir); err == nil && matched {
			seen[dir] = true
			out = append(out, Glob{0, dir, false}) // for directories we leave id=0
			continue
		}

		if matched, err := filepath.Match(query, p); err == nil && matched {
			seen[p] = true
			out = append(out, Glob{MetricID(id), p, true})
		}
	}

	return out
}

func (l *Idx) List(org int) []MetricID {
	var response []MetricID
	fn := func(k string, v interface{}) bool {
		response = append(response, v.(MetricID))
		return false
	}
	if org == -1 {
		for i := 0; i < len(l.prefix); i++ {
			l.prefix[i].Walk(fn)
		}
	} else {
		l.Prefix(-1, "", fn)
		l.Prefix(org, "", fn)
	}
	return response
}
func (i *Idx) QueryRadix(org int, query string) []Glob {
	var response []Glob
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
		if !seen[m] {
			seen[m] = true
			response = append(response, Glob{Id: v.(MetricID), Metric: m, IsLeaf: leaf})
		}
		// false == "don't terminate iteration"
		return false
	}
	i.Prefix(-1, query, fn)
	i.Prefix(org, query, fn)
	return response
}

// TODO(dgryski): this needs most of the logic in grobian/carbsonerver:findHandler()
// Dieter: this doesn't support {, }, [, ]
// output is unsorted, if you want it sorted, do it yourself
func (i *Idx) Match(org int, query string) (MatchType, []Glob) {

	// no wildcard == exact match only
	var star int
	if star = strings.Index(query, "*"); star == -1 {
		var id MetricID
		var ok bool
		if id, ok = i.Get(org, query); !ok {
			return MatchLiteral, nil
		}
		return MatchLiteral, []Glob{{Id: id, Metric: query, IsLeaf: true}}
	}

	var response []Glob
	var matchType MatchType

	if star == len(query)-1 {
		// only one trailing star
		query = query[:len(query)-1]
		response = i.QueryRadix(org, query)
		matchType = MatchPrefix
	} else {
		// at least one interior star
		response = i.QueryPath(org, query)
		matchType = MatchTrigram
	}

	return matchType, response
}

func extractTrigrams(query string) []trigram.T {

	if len(query) < 3 {
		return nil
	}

	var start int
	var i int

	var trigrams []trigram.T

	for i < len(query) {
		if query[i] == '[' || query[i] == '*' || query[i] == '?' {
			trigrams = trigram.Extract(query[start:i], trigrams)

			if query[i] == '[' {
				for i < len(query) && query[i] != ']' {
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
	maxDocs := int(pct * float64(len(i.Pathidx[trigram.TAllDocIDs])))
	for k, v := range i.Pathidx {
		// all values >= 1<<31 are special trigrams that should be kept
		if uint32(k) < uint32(2147483648) && len(v) > maxDocs {
			i.Pathidx[k] = nil
		}
	}
}
