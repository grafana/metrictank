Thanks for your interest in contributing to metrictank!

# Tooling

* [dep](https://github.com/golang/dep) for managing vendored dependencies
* `make` to build binaries and docker image.
* [metrics2docs](https://github.com/Dieterbe/metrics2docs) generates the metrics documentation for the [metrics page](https://github.com/grafana/metrictank/blob/master/docs/metrics.md) though it is buggy and manual intervention is needed, for now can just manually update the metrics page.

# Building, testing and QA

* `make bin`: for building all binaries.
* `make docker`: for building docker image (after building binaries)
* `make test`: unit tests
* `make qa`: run all QA (code quality) tests, same as CirceCI qa checks except CI also runs stack tests.

see the [Makefile](../Makefile) for more targets

# When contributing PR's

1. functions, methods, types should be clearly understandable, either through an obvious name, or documentation when needed.
2. all code documentation must conform to [golang best practices](https://blog.golang.org/godoc-documenting-go-code)
3. add unit tests for tricky or non-trivial code. Not needed for obvious, simple or glue code.  Use your best judgment or just ask us.
4. add benchmarks for performance sensitive functionality (e.g. code in the read and write path) or commonly executed code. (e.g. index inserts and searches)
   ([how to write benchmarks tutorial](https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go))
5. It's OK for your PR to not be based on the absolute latest master, but don't let the base of your branch get too out of date. (rule of thumb: no more than 40 commits or 2 weeks behind)
6. never merge master into your PR. As it mangles up git history and makes things harder to review. Instead, rebase your PR on top of master.
7. code must be well formatted. (checked via `scripts/qa/gofmt.sh`, `make qa` and circleCI)
8. regarding configuration:
   * config options and metrictank-sample.ini must be in sync. the latter must match the former.
   * all other config files should also be in sync with metrictank-sample.ini except where the file should differ on purpose.
   (example: `docker/docker-cluster/metrictank.ini` is the same as metrictank-sample.ini except for the options that support the use case of running metrictank in a cluster)
   Use `scripts/dev/sync-configs.sh` which helps with the process of updating all configs based on metrictank-sample.ini.
   Use `scripts/dev/config-to-doc.sh > docs/config.md` to sync the sample configuration into the documentation. (checked via `qa/docs.sh` , `make qa` and circleCI)
9. Any new tools must be properly documented. Use `scripts/dev/tools-to-doc.sh > docs/tools.md`. (checked via `qa/docs.sh` , `make qa` and circleCI)
10. PR's will only be merged if all tests pass
11. Any errors which can be recovered from sanely, must do so. And must trigger a high-level recovery metric (see other recovered_errors metrics) and an error message describing the problem.
    In other words, panic is only legal in unrecoverable, unexpected situations.
12. When defining multiple functions or structures stick to this ordering:
    * first the main structure, then other structures that it references (unless they belong in a different file or package of course)
    * first the high-level function, then any other function that it calls (unless it belongs in a different file or package of course)
    * first a structure, then its constructor, and then its methods
13. Documentation in the [docs](../docs) folder should be kept in sync with current state of affairs
14. HTTP status codes: use the [named codes defined in the standard library](https://golang.org/pkg/net/http/#pkg-constants) for easy searchability

# Documentation

* [docs](../docs) for users and operators
* [devdocs](../devdocs) for MT developers

