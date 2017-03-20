package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/raintank/metrictank/api"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

// mode must be normal or summary
func catId(id string, ttl, fromUnix, toUnix, fix uint32, mode string, store mdata.Store) {
	if fix != 0 {
		points := getSeries(id, ttl, fromUnix, toUnix, fix, store)

		switch mode {
		case "normal":
			printPointsNormal(points, fromUnix, toUnix)
		case "summary":
			printPointsSummary(points, fromUnix, toUnix)
		}
	} else {
		igens, err := store.Search(id, ttl, fromUnix, toUnix)
		if err != nil {
			panic(err)
		}

		switch mode {
		case "normal":
			printNormal(igens, fromUnix, toUnix)
		case "summary":
			printSummary(igens, fromUnix, toUnix)
		}
	}
}

func getSeries(id string, ttl, fromUnix, toUnix, interval uint32, store mdata.Store) []schema.Point {
	itgens, err := store.Search(id, ttl, fromUnix, toUnix)
	if err != nil {
		panic(err)
	}

	var points []schema.Point

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
		fmt.Printf("## chunk %d (span %d)\n", i, ig.Span)
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
	printTime := func(ts uint32) string {
		if *printTs {
			return fmt.Sprintf("%d", ts)
		} else {
			return time.Unix(int64(ts), 0).Format(tsFormat)
		}
	}
	if in {
		if nan {
			fmt.Println("> ", printTime(ts), "NAN")
		} else {
			fmt.Println("> ", printTime(ts), val)
		}
	} else {
		if nan {
			fmt.Println("- ", printTime(ts), "NAN")
		} else {
			fmt.Println("- ", printTime(ts), val)
		}
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
