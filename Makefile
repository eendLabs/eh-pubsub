default: test_docker

test:
	go test ./...
.PHONY: test

test_docker:
	docker-compose -f ./deployments/docker-compose.yaml run --rm app make test
	docker-compose -f ./deployments/docker-compose.yaml down
.PHONY: test_docker

cover:
	go list -f '{{if len .TestGoFiles}}"go test -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}"{{end}}' ./... | xargs -L 1 sh -c
.PHONY: cover

cover_docker:
	docker-compose -f ./deployments/docker-compose.yaml run --rm app make cover
	docker-compose -f ./deployments/docker-compose.yaml down
.PHONY: cover_docker

publish_cover: cover_docker
	go get -d golang.org/x/tools/cmd/cover
	go get github.com/modocache/gover
	go get github.com/mattn/goveralls
	gover
	@goveralls -coverprofile=gover.coverprofile -service=travis-ci \
		-repotoken=$(COVERALLS_TOKEN)
.PHONY: publish_cover

services:
	#docker-compose -f ./deployments/docker-compose.yaml build pubsub_emulator
	docker-compose -f ./deployments/docker-compose.yaml up -d pubsub_emulator
.PHONY: services

stop:
	docker-compose -f ./deployments/docker-compose.yaml down
.PHONY: stop

clean:
	@find . -name \.coverprofile -type f -delete
	@rm -f gover.coverprofile
.PHONY: clean