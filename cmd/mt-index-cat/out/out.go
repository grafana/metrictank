package out

import (
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/davecgh/go-spew/spew"

	"github.com/raintank/schema"
)

var QueryTime int64

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

func Template(format string) func(d schema.MetricDefinition) {
	funcs := make(map[string]interface{})
	funcs["pattern"] = pattern
	funcs["patternCustom"] = patternCustom
	funcs["age"] = age
	funcs["roundDuration"] = roundDuration

	// replace '\n' in the format string with actual newlines.
	format = strings.Replace(format, "\\n", "\n", -1)

	tpl := template.Must(template.New("format").Funcs(funcs).Parse(format))

	return func(d schema.MetricDefinition) {
		err := tpl.Execute(os.Stdout, d)
		if err != nil {
			panic(err)
		}
	}
}
