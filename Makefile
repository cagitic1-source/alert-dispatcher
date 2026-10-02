MODULE := github.com/cagitic1-source/alert-dispatcher

.PHONY: gen run db-up db-down test

gen:
	protoc \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		api/proto/alert/v1/alert.proto

run:
	-go run ./cmd

db-up:
	docker compose up -d

db-down:
	docker compose down


test:
	go test ./... -race
