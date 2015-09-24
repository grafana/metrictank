how to test:

* spin up dev stack
* add new endpoint, localhost, default settings are fine.
* you can now query for `http://localhost:6063/get?render=litmus.localhost.dev1.dns.ok_state`
* optionally, specify `from` and `to` unix timestamps. from defaults to 24h ago, to to now.
* note: just serves up the data that it has, in timestamp ascending order. it does no effort to try to fill in gaps.
* you can also specify multiple series like so
`http://localhost:6063/get?render=litmus.localhost.dev1.dns.warn_state&render=litmus.localhost.dev1.dns.ok_state&from=1443099995`
* note: no support for wildcards, patterns, "magic" time specs like "-10min" etc.
