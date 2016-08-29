.PHONY: test bin docker
default:
	$(MAKE) all
test:
	bash -c "./scripts/test.sh $(TEST)"
check:
	$(MAKE) test
bin:
	bash -c "./scripts/build.sh"
docker:
	bash -c "./scripts/build_docker.sh"
all:
	$(MAKE) bin
	$(MAKE) docker
