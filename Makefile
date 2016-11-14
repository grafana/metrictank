.PHONY: test bin docker
default:
	$(MAKE) all
test:
	./scripts/test.sh
check:
	$(MAKE) test
bin:
	./scripts/build.sh
docker:
	./scripts/build_docker.sh
all:
	$(MAKE) bin
	$(MAKE) docker
