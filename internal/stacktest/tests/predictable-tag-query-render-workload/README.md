This tool exerts a consistent workload of tagqueries on a graphite endpoint.

It expects queries for fakemetrics id's like `123456.*` to result in 123456, 1234560 through 1234569
which you can achieve with an ingest workload such as:

```
fakemetrics feed --period 60s --mpo 1800000 --add-tags --num-unique-tags 3
```
