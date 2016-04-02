// The idx package provides a metadata index for metrics
// it's based on the index from https://github.com/dgryski/carbonmem/
// it seems promising and performant for our current needs, but also experimental
// We don't necessarily want to start maintaining our own little search engine.
// Future needs (scale, features) may deprecate this package in favor of something else
// like ES, bleve, etc.  But for now it seems to work well, so let's see where it takes us

// callers are responsible for thread safety, period pruning if desired
package idx

import (
	//"fmt"
	"path/filepath"
	"sort"
	"strings"
	//	"time"

	"github.com/armon/go-radix"
	"github.com/dgryski/go-trigram"
)

type MetricID uint32 // same thing as trigram.DocID. just syntactic sugar

type Glob struct {
	Metric string
	IsLeaf bool
}

type globByName []Glob

func (g globByName) Len() int           { return len(g) }
func (g globByName) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g globByName) Less(i, j int) bool { return g[i].Metric < g[j].Metric }

type Idx struct {
	// all metrics
	keys  map[string]MetricID
	revs  []string
	count int

	// currently 'active'
	active map[MetricID]int
	prefix *radix.Tree

	Pathidx trigram.Index
}

func New() *Idx {
	return &Idx{
		keys: make(map[string]MetricID),

		active: make(map[MetricID]int),
		prefix: radix.New(),

		Pathidx: trigram.NewIndex(nil),
	}
}

func (l *Idx) Len() int {
	return len(l.keys)
}

func (l *Idx) Get(key string) (MetricID, bool) {
	id, ok := l.keys[key]
	return id, ok
}

func (l *Idx) GetOrAdd(key string) MetricID {

	id, ok := l.keys[key]

	if ok {
		return id
	}

	id = MetricID(l.count)
	l.count++
	l.revs = append(l.revs, key)

	l.keys[key] = id

	l.Pathidx.Insert(key, trigram.DocID(id))

	return id
}

func (l *Idx) GetById(i MetricID) string {
	return l.revs[i]
}

func (l *Idx) Key(id MetricID) string {
	return l.revs[id]
}

func (l *Idx) AddRef(id MetricID) {
	v, ok := l.active[id]
	if !ok {
		l.prefix.Insert(l.revs[id], id)
	}

	l.active[id] = v + 1
}

func (l *Idx) DelRef(id MetricID) {
	l.active[id]--
	if l.active[id] == 0 {
		delete(l.active, id)
		delete(l.keys, l.revs[id])
		l.prefix.Delete(l.revs[id])
		if l.Pathidx != nil {
			l.Pathidx.Delete(l.revs[id], trigram.DocID(id))
		}
		l.revs[id] = ""
	}
}

func (l *Idx) Active(id MetricID) bool {
	return l.active[id] != 0
}

func (l *Idx) Prefix(query string, fn radix.WalkFn) {
	l.prefix.WalkPrefix(query, fn)
}

// for a "filesystem glob" query (e.g. supports * and [] but not {})
// looks in the trigrams index
func (l *Idx) QueryPath(query string) []Glob {

	if l.Pathidx == nil {
		return nil
	}

	ts := extractTrigrams(query)

	//pre := time.Now()
	ids := l.Pathidx.QueryTrigrams(ts)
	//fmt.Println("QueryTrigrams took", time.Now().Sub(pre))

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
			out = append(out, Glob{dir, false})
			continue
		}

		if matched, err := filepath.Match(query, p); err == nil && matched {
			seen[p] = true
			out = append(out, Glob{p, true})
		}
	}

	return out
}

// TODO(dgryski): this needs most of the logic in grobian/carbsonerver:findHandler()
// Dieter: this doesn't support {, }, [, ]

func (i *Idx) Match(query string) []Glob {

	// no wildcard == exact match only
	var star int
	if star = strings.Index(query, "*"); star == -1 {
		if _, ok := i.Get(query); !ok {
			return nil
		}
		return []Glob{{Metric: query, IsLeaf: true}}
	}

	var response []Glob

	if star == len(query)-1 {
		// only one trailing star
		query = query[:len(query)-1]

		l := len(query)
		seen := make(map[string]bool)
		i.Prefix(query, func(k string, v interface{}) bool {
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
				response = append(response, Glob{Metric: m, IsLeaf: leaf})
			}
			// false == "don't terminate iteration"
			return false
		})
	} else {
		// at least one interior star
		response = i.QueryPath(query)
	}

	sort.Sort(globByName(response))

	return response
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

func (i *Idx) Prune(pct float64) int {
	return i.Pathidx.Prune(pct)
}
