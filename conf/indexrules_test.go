package conf

import (
	"io/ioutil"
	"regexp"
	"testing"
	"time"
)

func TestReadIndexRules(t *testing.T) {
	cases := []struct {
		in       string
		expErr   bool
		expRules IndexRules
	}{
		{
			in: `
[default]
pattern = 
max-stale = 0
`,
			expErr: false,
			expRules: IndexRules{
				Rules: []IndexRule{
					{
						Name:     "default",
						Pattern:  regexp.MustCompile(""),
						MaxStale: 0,
					},
				},
				Default: IndexRule{
					Name:     "default",
					Pattern:  regexp.MustCompile(""),
					MaxStale: 0,
				},
			},
		},
		{
			in: `
[longterm]
pattern = ^long
max-stale = 1y6mon

[default]
pattern = foobar
max-stale = 7d
`,
			expErr: false,
			expRules: IndexRules{
				Rules: []IndexRule{
					{
						Name:     "longterm",
						Pattern:  regexp.MustCompile("^long"),
						MaxStale: time.Duration(365+6*30) * 24 * time.Hour,
					},
					{
						Name:     "default",
						Pattern:  regexp.MustCompile("foobar"),
						MaxStale: time.Duration(24*7) * time.Hour,
					},
				},
				Default: IndexRule{
					Name:     "default",
					Pattern:  regexp.MustCompile(""),
					MaxStale: 0,
				},
			},
		},
	}
	for i, c := range cases {
		err := ioutil.WriteFile("/tmp/indexrules-test-readindexrules", []byte(c.in), 0644)
		if err != nil {
			panic(err)
		}
		rules, err := ReadIndexRules("/tmp/indexrules-test-readindexrules")
		if (err != nil) != c.expErr {
			t.Fatalf("case %d, exp err %t, got err %v", i, c.expErr, err)
		}
		if err == nil {
			if len(c.expRules.Rules) != len(rules.Rules) {
				t.Fatalf("case %d, exp rules %v, got %v", i, c.expRules, rules)
			}
			for i, expRule := range c.expRules.Rules {
				rule := rules.Rules[i]
				if rule.Name != expRule.Name || rule.Pattern.String() != expRule.Pattern.String() || rule.MaxStale != expRule.MaxStale {
					t.Fatalf("case %d, exp rules %v, got %v", i, c.expRules, rules)
				}
			}
			rule := rules.Default
			expRule := c.expRules.Default
			if rule.Name != expRule.Name || rule.Pattern.String() != expRule.Pattern.String() || rule.MaxStale != expRule.MaxStale {
				t.Fatalf("case %d, exp rules %v, got %v", i, c.expRules, rules)
			}
		}
	}
}
func TestIndexRulesMatch(t *testing.T) {
	rules := IndexRules{
		Rules: []IndexRule{
			{
				Name:     "longterm",
				Pattern:  regexp.MustCompile("^long"),
				MaxStale: time.Duration(365+6*30) * 24 * time.Hour,
			},
			// default provided by user
			{
				Name:     "default",
				Pattern:  regexp.MustCompile("foobar"),
				MaxStale: time.Duration(24*7) * time.Hour,
			},
		},
		// built-in to the software
		Default: IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}
	cases := []struct {
		metric    string
		expRuleID uint16
	}{
		{
			"abced.f",
			2,
		},
		{
			"long.baz",
			0,
		},
		{
			"long.foobar.f", // rule 0 takes precedence over rule 1 !
			0,
		},
		{
			"abc.foobar.baz",
			1,
		},
	}
	for i, c := range cases {
		ruleID, _ := rules.Match(c.metric)
		if ruleID != c.expRuleID {
			t.Fatalf("mismatch for case %d: exp ruleID %d, got %d", i, c.expRuleID, ruleID)
		}
	}

}
