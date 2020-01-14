Thanks for your interest in contributing to Metrictank!

# Tooling

* [dep](https://github.com/golang/dep) for managing vendored dependencies
* `make` to build binaries and Docker image
* [metrics2docs](https://github.com/Dieterbe/metrics2docs) generates the metrics documentation for the [metrics page](https://github.com/grafana/metrictank/blob/master/docs/metrics.md)

# Building, testing, and QA

* `make bin`: for building all binaries
* `make docker`: for building docker image (after building binaries)
* `make test`: unit tests excluding expensive integration tests (faster)
* `make test-all`: unit tests including expensive integration tests (slower)
* `make qa`: run all QA (code quality) tests, same as CirceCI qa checks except CI also runs stack tests

See the [Makefile](../Makefile) for more targets

# When contributing PRs

1. Functions, methods, types should be clearly understandable, either through an obvious name, or documentation when needed.
2. All code documentation must conform to [golang best practices](https://blog.golang.org/godoc-documenting-go-code)
3. Add unit tests for tricky or non-trivial code. Not needed for obvious, simple or glue code.  Use your best judgment or just ask us.
   For bugfixes, construct the git history such that the commit introducing the test comes before the bugfix, this makes it much easier to validate the fix.
4. Add benchmarks for performance sensitive functionality (e.g. code in the read and write path) or commonly executed code. (e.g. index inserts and searches)
   ([how to write benchmarks tutorial](https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go))
5. It's okay for your PR to not be based on the absolute latest master, but don't let the base of your branch get too out of date. (rule of thumb: no more than 40 commits or two weeks behind)
6. Never merge master into your PR. As it mangles Git history and makes things harder to review. Instead, rebase your PR on top of master.
7. Code must be well formatted. It is checked via `scripts/qa/gofmt.sh`, `make qa`, and circleCI.
8. Regarding configuration:
   * Configuration options and metrictank-sample.ini must be in sync.
   * All other configuration files should also be in sync with metrictank-sample.ini except where the file should differ on purpose.
   (Example: `docker/docker-cluster/metrictank.ini` is the same as metrictank-sample.ini except for the options that support the use case of running metrictank in a cluster.)
   Use `scripts/dev/sync-configs.sh` which helps with the process of updating all configs based on metrictank-sample.ini.
   Use `scripts/dev/config-to-doc.sh > docs/config.md` to sync the sample configuration into the documentation. It is checked via `qa/docs.sh` , `make qa`, and circleCI.
9. Any new tools must be properly documented. Use `scripts/dev/tools-to-doc.sh > docs/tools.md`. It is checked via `qa/docs.sh` , `make qa`, and circleCI.
10. PR's will only be merged if all tests pass
11. Any errors which can be recovered from sanely, must do so. And must trigger a high-level recovery metric (see other recovered_errors metrics) and an error message describing the problem. In other words, panic is only legal in unrecoverable, unexpected situations.
12. When defining multiple functions or structures stick to this ordering:
    * First the main structure, then other structures that it references (unless they belong in a different file or package of course)
    * First the high-level function, then any other function that it calls (unless it belongs in a different file or package of course)
    * First a structure, then its constructor, and then its methods
13. Documentation in the [docs](../docs) folder should be kept in sync with current state of affairs.
14. HTTP status codes: use the [named codes defined in the standard library](https://golang.org/pkg/net/http/#pkg-constants) for easy searchability.
15. Don't rely on `.String()` being called implicitly, instead write the call out. This is to prevent that adding a `.Error()` method to structs can unexpectedly change the printed values.
16. Any change which requires attention upon upgrade, must come with a corresponding mention in the CHANGELOG.md, right under the 'unreleased/master' header.

# Documentation

* [docs](../docs) for users, operators and basic development instructions
* [devdocs](../devdocs) in-depth implementation details for developers

# Release process

During normal development, maintain CHANGELOG.md, and mark interesting - to users and operators - changes under "unreleased" version.
* Include: external API changes, performance improvements, features, anything affecting operations, etc.
* Exclude: internal API changes, code refactorings/clean-ups, changes to the build process, etc. People interested in these have the git history.

Grafana Labs regularly deploys the latest code from `master`, but cannot possibly do extensive testing of all functionality in production, so users are encouraged to run master also, and report any issues they hit.
When significant changes have been merged to master, and they have had a chance to be tested or run in production for a while, we tag a release, as follows:

* Update CHANGELOG.md from `unreleased` to the version, and add the date.
* Create annotated git tag in the form `v<version>` and push to GitHub.
* Wait for CircleCI to complete successfully.
* Create release on GitHub. copy entry from CHANGELOG.md to GitHub release page.
