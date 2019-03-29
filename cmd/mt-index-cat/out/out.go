package out

import (
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/raintank/schema"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/idx"
)

var QueryTime int64

func convertIdxMdToSchemaMd(d idx.MetricDefinition) schema.MetricDefinition {
	md := schema.MetricDefinition{
		Id:         d.Id,
		OrgId:      d.OrgId,
		Name:       d.Name.String(),
		Interval:   d.Interval,
		Unit:       d.Unit,
		Mtype:      d.Mtype(),
		Tags:       d.Tags.Strings(),
		LastUpdate: d.LastUpdate,
		Partition:  d.Partition,
	}
	md.NameWithTags() //ensure nameWithTags is set
	return md
}

func Dump(d idx.MetricDefinition) {
	spew.Dump(convertIdxMdToSchemaMd(d))
}

<<<<<<< HEAD
func List(d schema.MetricDefinition) {
	fmt.Println(d.OrgId, d.NameWithTags())
=======
func List(d idx.MetricDefinition) {
	fmt.Println(d.OrgId, d.Name.String())
>>>>>>> update mt-index-cat
}

func GetVegetaRender(addr, from string) func(d idx.MetricDefinition) {
	return func(d idx.MetricDefinition) {
		fmt.Printf("GET %s/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", addr, d.Name.String(), from, d.OrgId)
	}
}

func GetVegetaRenderPattern(addr, from string) func(d idx.MetricDefinition) {
	return func(d idx.MetricDefinition) {
		fmt.Printf("GET %s/render?target=%s&from=-%s\nX-Org-Id: %d\n\n", addr, pattern(d.Name.String()), from, d.OrgId)
	}
}

func Template(format string) func(d idx.MetricDefinition) {
	funcs := make(map[string]interface{})
	funcs["pattern"] = pattern
	funcs["patternCustom"] = patternCustom
	funcs["age"] = age
	funcs["roundDuration"] = roundDuration

	// replace '\n' in the format string with actual newlines.
	format = strings.Replace(format, "\\n", "\n", -1)

	tpl := template.Must(template.New("format").Funcs(funcs).Parse(format))

	return func(d idx.MetricDefinition) {
		md := convertIdxMdToSchemaMd(d)
		err := tpl.Execute(os.Stdout, &md)
		if err != nil {
			panic(err)
		}
	}
}
