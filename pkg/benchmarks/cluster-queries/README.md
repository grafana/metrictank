## cluster-queries benchmark

## goal

realistic query workload to see
where we allocate (and keep in use) significant amounts of memory

in particular for:
1) optimizing pointSlicePool
2) optimizing chunkcache

## running the benchmark

```
./launch.sh docker-cluster
```

wait for stack to be up.

```
./fakemetrics backfill --kafka-mdm-addr localhost:9092 --kafka-mdm-v2=false --offset $((2*366*24))h --period 1800s --speedup 360000 --mpo 500
```

this should complete in about 3 minutes.
load MT dashboard and confirm it ingested data at 200kHz for 3 minutes

when done: in grafana look at `some.id.of.a.metric.{1,72,199,320,438};id=*;some=tag` to confirm data looks good (over 2y)

finally: `./run.sh`

## observations:

* memory usage grows during test but does not release afterwards (!).
  in line with cache, but much more than cache reports (~50MB). profile shows ~50~60% of allocations are in chunk cache


