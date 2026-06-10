PROTO_DIR := api/proto/auth
BIN       := bin

.PHONY: proto build server client docker-up docker-down logs clean

proto:
	protoc \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  -I . $(PROTO_DIR)/*.proto

build: proto
	mkdir -p $(BIN)
	go build -o $(BIN)/server ./cmd/server
	go build -o $(BIN)/client ./cmd/client

server: proto
	go build -o $(BIN)/server ./cmd/server

client: proto
	go build -o $(BIN)/client ./cmd/client

docker-up:
	docker-compose up --build -d
	@echo "waiting for cluster..."
	@sleep 8
	@docker-compose logs --tail=20 node1

docker-down:
	docker-compose down -v

logs:
	docker-compose logs -f

clean:
	rm -rf $(BIN)
	find . -name "*.pb.go" -delete
