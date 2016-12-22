package out

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/davecgh/go-spew/spew"

	"gopkg.in/raintank/schema.v1"
)

func Dump(d schema.MetricDefinition) {
	spew.Dump(d)
}

func List(d schema.MetricDefinition) {
	fmt.Println(d.OrgId, d.Name)
}

func GetVegetaRender(addr, from string) func(d schema.MetricDefinition) {
	return func(d schema.MetricDefinition) {
		fmt.Printf("GET %s/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", addr, d.Name, from, d.OrgId)
	}
}

func GetVegetaRenderPattern(addr, from string) func(d schema.MetricDefinition) {
	return func(d schema.MetricDefinition) {
		fmt.Printf("GET %s/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", addr, pattern(d.Name), from, d.OrgId)
	}
}

func pattern(in string) string {
	mode := rand.Intn(3)
	if mode == 0 {
		// in this mode, replaces a node with a wildcard
		parts := strings.Split(in, ".")
		parts[rand.Intn(len(parts))] = "*"
		return strings.Join(parts, ".")
	} else if mode == 1 {
		// randomly replace chars with a *
		// note that in 1/5 cases, nothing happens
		// and otherwise, sometimes valid patterns are produced,
		// but it's also possible to produce patterns that won't match anything (if '.' was taken out)
		chars := rand.Intn(5)
		pos := rand.Intn(len(in) - chars)
		return in[0:pos] + "*" + in[pos+chars:]
	}
	// mode 3: do nothing :)
	return in
}
