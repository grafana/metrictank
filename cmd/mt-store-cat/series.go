package main

import (
	"context"
	"fmt"
	"math"
	"os"

	"github.com/grafana/metrictank/api"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/schema"
)

// printPoints prints points in the store corresponding to the given requirements
func printPoints(ctx context.Context, store *cassandra.CassandraStore, tables []cassandra.Table, metrics []Metric, fromUnix, toUnix, fix uint32) {
	for _, metric := range metrics {
		fmt.Println("## Metric", metric)
		for _, table := range tables {
			fmt.Println("### Table", table.Name)
			if fix != 0 {
				points := getSeries(ctx, store, table, metric.AMKey, fromUnix, toUnix, fix)
				printPointsNormal(points, fromUnix, toUnix)
			} else {
				igens, err := store.SearchTable(ctx, metric.AMKey, table, fromUnix, toUnix)
				if err != nil {
					panic(err)
				}
				printNormal(igens, fromUnix, toUnix)
			}
		}
	}
}

// printPointSummary prints a summarized view of the points in the store corresponding to the given requirements
func printPointSummary(ctx context.Context, store *cassandra.CassandraStore, tables []cassandra.Table, metrics []Metric, fromUnix, toUnix, fix uint32) {
	for _, metric := range metrics {
		fmt.Println("## Metric", metric)
		for _, table := range tables {
			fmt.Println("### Table", table.Name)
			if fix != 0 {
				points := getSeries(ctx, store, table, metric.AMKey, fromUnix, toUnix, fix)
				printPointsSummary(points, fromUnix, toUnix)
			} else {
				igens, err := store.SearchTable(ctx, metric.AMKey, table, fromUnix, toUnix)
				if err != nil {
					panic(err)
				}
				printSummary(igens, fromUnix, toUnix)
			}
		}
	}
}

func getSeries(ctx context.Context, store *cassandra.CassandraStore, table cassandra.Table, amkey schema.AMKey, fromUnix, toUnix, interval uint32) []schema.Point {
	var points []schema.Point
	itgens, err := store.SearchTable(ctx, amkey, table, fromUnix, toUnix)
	if err != nil {
		panic(err)
	}

	for i, itgen := range itgens {
		iter, err := itgen.Get()
		if err != nil {
			fmt.Fprintf(os.Stderr, "chunk %d itergen.Get: %s", i, err)
			continue
		}
		for iter.Next() {
			ts, val := iter.Values()
			if ts >= fromUnix && ts < toUnix {
				points = append(points, schema.Point{Val: val, Ts: ts})
			}
		}
	}
	return api.Fix(points, fromUnix, toUnix, interval)
}

func printNormal(igens []chunk.IterGen, from, to uint32) {
	fmt.Println("number of chunks:", len(igens))
	for i, ig := range igens {
		fmt.Printf("#### chunk %d (t0:%s, span:%d)\n", i, printTime(ig.T0), ig.Span)
		iter, err := ig.Get()
		if err != nil {
			fmt.Fprintf(os.Stderr, "chunk %d itergen.Get: %s", i, err)
			continue
		}
		for iter.Next() {
			ts, val := iter.Values()
			printRecord(ts, val, ts >= from && ts < to, math.IsNaN(val))
		}
	}
}

func printPointsNormal(points []schema.Point, from, to uint32) {
	fmt.Println("number of points:", len(points))
	for _, p := range points {
		printRecord(p.Ts, p.Val, p.Ts >= from && p.Ts < to, math.IsNaN(p.Val))
	}
}

func printRecord(ts uint32, val float64, in, nan bool) {
	prefix := "- "
	if in {
		prefix = "> "
	}
	if nan {
		fmt.Println(prefix, printTime(ts), "NAN")
	} else {
		fmt.Println(prefix, printTime(ts), val)
	}
}

func printSummary(igens []chunk.IterGen, from, to uint32) {

	var count int
	first := true
	var prevIn, prevNaN bool
	var ts uint32
	var val float64

	var followup = func(count int, in, nan bool) {
		fmt.Printf("... and %d more of in_range=%t nan=%t ...\n", count, in, nan)
	}

	for i, ig := range igens {
		iter, err := ig.Get()
		if err != nil {
			fmt.Fprintf(os.Stderr, "chunk %d itergen.Get: %s", i, err)
			continue
		}
		for iter.Next() {
			ts, val = iter.Values()

			nan := math.IsNaN(val)
			in := (ts >= from && ts < to)

			if first {
				printRecord(ts, val, in, nan)
			} else if nan == prevNaN && in == prevIn {
				count++
			} else {
				followup(count, prevIn, prevNaN)
				printRecord(ts, val, in, nan)
				count = 0
			}

			prevNaN = nan
			prevIn = in
			first = false
		}
	}
	if count > 0 {
		followup(count, prevIn, prevNaN)
		fmt.Println("last value was:")
		printRecord(ts, val, prevIn, prevNaN)
	}
}

func printPointsSummary(points []schema.Point, from, to uint32) {

	var count int
	first := true
	var prevIn, prevNaN bool
	var ts uint32
	var val float64

	var followup = func(count int, in, nan bool) {
		fmt.Printf("... and %d more of in_range=%t nan=%t ...\n", count, in, nan)
	}

	for _, p := range points {
		ts, val = p.Ts, p.Val

		nan := math.IsNaN(val)
		in := (ts >= from && ts < to)

		if first {
			printRecord(ts, val, in, nan)
		} else if nan == prevNaN && in == prevIn {
			count++
		} else {
			followup(count, prevIn, prevNaN)
			printRecord(ts, val, in, nan)
			count = 0
		}

		prevNaN = nan
		prevIn = in
		first = false
	}
	if count > 0 {
		followup(count, prevIn, prevNaN)
		fmt.Println("last value was:")
		printRecord(ts, val, prevIn, prevNaN)
	}
}
