BUILD_GIT_HASH ?= $(shell git rev-parse HEAD)
BUILD_VERSION = $(shell echo ${BUILD_GIT_HASH} | cut -c 1-12)
BUILD_DIR = build
DOCKER_COMPOSE_FILE = deployments/docker-compose.yml

default: build

.PHONY: build compose-rebuild compose-up compose-up-clean compose-down compose-logs help
build: ## build executable
	GO111MODULE=on GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
	go build -ldflags "-s -X main.version=${BUILD_VERSION}" \
	-o "${BUILD_DIR}/eventagg" cmd/eventagg/main.go

compose-rebuild: ## rebuild docker images for local docker-compose
	docker-compose -f ${DOCKER_COMPOSE_FILE} build

compose-up: ## run local eventagg environment
	docker-compose -f ${DOCKER_COMPOSE_FILE} up -d

compose-up-clean: compose-rebuild compose-up ## rebuild and then up local eventagg environment

compose-down: ## stop and remove local docker-compose environment
	docker-compose -f ${DOCKER_COMPOSE_FILE} down

compose-logs: ## see running service logs
	docker-compose -f ${DOCKER_COMPOSE_FILE} logs -f --tail=100

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; { printf "\033[36m%-20s\033[0m - %s\n", $$1, $$2 }'
