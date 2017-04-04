package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/raintank/metrictank/mdata"
)

type TablesByTTL []string

func (t TablesByTTL) Len() int      { return len(t) }
func (t TablesByTTL) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t TablesByTTL) Less(i, j int) bool {
	iTTL, _ := strconv.Atoi(strings.Split(t[i], "_")[1])
	jTTL, _ := strconv.Atoi(strings.Split(t[j], "_")[1])
	return iTTL < jTTL
}

func getTables(store *mdata.CassandraStore, keyspace string, match string) ([]string, error) {
	var tables []string
	meta, err := store.Session.KeyspaceMetadata(keyspace)
	if err != nil {
		return tables, err
	}
	if match == "*" || match == "" {
		for tbl := range meta.Tables {
			if tbl == "metric_idx" || !strings.HasPrefix(tbl, "metric_") {
				continue
			}
			tables = append(tables, tbl)
		}

		sort.Sort(TablesByTTL(tables))
	} else {
		if _, ok := meta.Tables[match]; !ok {
			return nil, fmt.Errorf("table %q not found", match)
		}
		tables = append(tables, match)
	}
	return tables, nil
}
