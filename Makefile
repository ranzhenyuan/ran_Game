.PHONY: tidy build run test vet fmt

tidy:
	go mod tidy

build:
	go build -o bin/server.exe ./cmd/server

run:
	go run ./cmd/server -config configs/server.yaml

test:
	go test -count=1 ./...

# race 检测需要 CGO 与 gcc（Windows 需安装 mingw-w64）
test-race:
	CGO_ENABLED=1 go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .
