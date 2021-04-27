SRC_FILES= catalog_tower_persister.go\
	   persister_worker.go \
	   kafka_listener.go

          
TEST_FILES= 

BINARY=catalog_tower_persister
.DEFAULT_GOAL := build

.PHONY: build
build:
	go build -o ${BINARY} ${SRC_FILES}

.PHONY: test
test:
	go test -race ./...

.PHONY: test_debug
test_debug:
	dlv debug ./...

.PHONY: coverage
coverage:
	rm -rf coverage.out
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: format
format:
	go fmt ${SRC_FILES} ${TEST_FILES}

.PHONY: run
run:
	go run ${SRC_FILES}

.PHONY: race
race:
	go run -race ${SRC_FILES}

.PHONY: debug
debug:
	dlv debug ${SRC_FILES}

.PHONY: linux
linux: 
	GOOS=linux GOARCH=arm go build -x -o catalog_worker.linux ${SRC_FILES}

.PHONY: clean
clean:
	go clean

.PHONY: lint
lint:
	golint ./...
