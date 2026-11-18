.PHONY: test integration bench fuzz lint run up down load

test:
	go test -race -cover ./...

integration:
	go test -race -tags integration ./...

bench:
	go test -tags integration -run '^$$' -bench . ./internal/limiter/

fuzz:
	go test -run '^$$' -fuzz FuzzParseRules -fuzztime 30s ./internal/config/

lint:
	gofmt -l . && go vet ./... && go vet -tags integration ./...

run:
	go run ./cmd/ratelimiter

up:
	docker compose up --build -d

down:
	docker compose down

load:
	k6 run loadtest/k6.js
