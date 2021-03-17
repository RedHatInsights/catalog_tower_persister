SRC_FILES= catalog_tower_persister.go\
	   persister_worker.go \
	   kafka_listener.go

          
TEST_FILES= 

BINARY=catalog_tower_persister
.DEFAULT_GOAL := build

build:
	go build -o ${BINARY} ${SRC_FILES}

test:
	go test -race ./...

test_debug:
	dlv debug ./...

coverage:
	rm -rf coverage.out
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

format:
	go fmt ${SRC_FILES} ${TEST_FILES}

run:
	go run ${SRC_FILES}

race:
	go run -race ${SRC_FILES}

debug:
	dlv debug ${SRC_FILES}

linux: 
	GOOS=linux GOARCH=arm go build -x -o catalog_worker.linux ${SRC_FILES}

clean:
	go clean

lint:
	golint ./...
