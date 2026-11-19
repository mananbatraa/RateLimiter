.PHONY: test integration topologies bench fuzz lint run up down load load-outage

test:
	go test -race -cover ./...

integration:
	go test -race -tags integration ./...

# Starts a local Redis Cluster + Sentinel, runs the topology tests, stops them.
topologies:
	scripts/redis-topologies.sh up > /tmp/rl-topologies.env
	. /tmp/rl-topologies.env && go test -race -tags integration -run Topology -v ./cmd/ratelimiter/; \
	  status=$$?; scripts/redis-topologies.sh down; exit $$status

bench:
	go test -tags integration -run '^$$' -bench . ./internal/limiter/

fuzz:
	go test -run '^$$' -fuzz FuzzParseRules -fuzztime 30s ./internal/config/

lint:
	gofmt -l . && go vet ./... && go vet -tags integration ./...
	golangci-lint run --build-tags integration ./...

run:
	go run ./cmd/ratelimiter

up:
	docker compose up --build -d

down:
	docker compose down

load:
	k6 run loadtest/k6.js

load-outage:
	./loadtest/outage.sh
