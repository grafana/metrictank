.PHONY: test bin docker
default:
	$(MAKE) all
test:
	CGO_ENABLED=1 go test -v -race $(go list ./... | grep -v /vendor/)
check:
	$(MAKE) test
bin:
	./scripts/build.sh
	./scripts/build_tools.sh
docker:
	./scripts/build_docker.sh
all:
	$(MAKE) bin
	$(MAKE) docker
