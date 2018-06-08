package cassandra

import (
	"fmt"
	"math"
)

type TTLTables map[uint32]ttlTable
type ttlTable struct {
	Table      string
	WindowSize uint32
}

func GetTTLTables(ttls []uint32, windowFactor int, nameFormat string) TTLTables {
	tables := make(TTLTables)
	for _, ttl := range ttls {
		tables[ttl] = GetTTLTable(ttl, windowFactor, nameFormat)
	}
	return tables
}

func GetTTLTable(ttl uint32, windowFactor int, nameFormat string) ttlTable {
	/*
	 * the purpose of this is to bucket metrics of similar TTLs.
	 * we first calculate the largest power of 2 that's smaller than the TTL and then divide the result by
	 * the window factor. for example with a window factor of 20 we want to group the metrics like this:
	 *
	 * generated with: https://gist.github.com/replay/69ad7cfd523edfa552cd12851fa74c58
	 *
	 * +------------------------+---------------+---------------------+----------+
	 * |              TTL hours |    table_name | window_size (hours) | sstables |
	 * +------------------------+---------------+---------------------+----------+
	 * |         0 <= hours < 1 |     metrics_0 |                   1 |    0 - 2 |
	 * |         1 <= hours < 2 |     metrics_1 |                   1 |    1 - 3 |
	 * |         2 <= hours < 4 |     metrics_2 |                   1 |    2 - 5 |
	 * |         4 <= hours < 8 |     metrics_4 |                   1 |    4 - 9 |
	 * |        8 <= hours < 16 |     metrics_8 |                   1 |   8 - 17 |
	 * |       16 <= hours < 32 |    metrics_16 |                   1 |  16 - 33 |
	 * |       32 <= hours < 64 |    metrics_32 |                   2 |  16 - 33 |
	 * |      64 <= hours < 128 |    metrics_64 |                   4 |  16 - 33 |
	 * |     128 <= hours < 256 |   metrics_128 |                   7 |  19 - 38 |
	 * |     256 <= hours < 512 |   metrics_256 |                  13 |  20 - 41 |
	 * |    512 <= hours < 1024 |   metrics_512 |                  26 |  20 - 41 |
	 * |   1024 <= hours < 2048 |  metrics_1024 |                  52 |  20 - 41 |
	 * |   2048 <= hours < 4096 |  metrics_2048 |                 103 |  20 - 41 |
	 * |   4096 <= hours < 8192 |  metrics_4096 |                 205 |  20 - 41 |
	 * |  8192 <= hours < 16384 |  metrics_8192 |                 410 |  20 - 41 |
	 * | 16384 <= hours < 32768 | metrics_16384 |                 820 |  20 - 41 |
	 * | 32768 <= hours < 65536 | metrics_32768 |                1639 |  20 - 41 |
	 * +------------------------+---------------+---------------------+----------+
	 */

	// calculate the pre factor window by finding the largest power of 2 that's smaller than ttl
	preFactorWindow := uint32(math.Exp2(math.Floor(math.Log2(ttlUnits(ttl)))))
	tableName := fmt.Sprintf(nameFormat, preFactorWindow)
	return ttlTable{
		Table:      tableName,
		WindowSize: preFactorWindow/uint32(windowFactor) + 1,
	}
}

func ttlUnits(ttl uint32) float64 {
	// convert ttl to hours
	return float64(ttl) / (60 * 60)
}
