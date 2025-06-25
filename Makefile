SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client movies-analysis/client
.PHONY: build

docker-image:
	docker build -f ./gateway/Dockerfile -t "gateway:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./results_tester/Dockerfile -t "results_tester:latest" .
	docker build -f ./controllers/preprocessors/movies_preprocessor/Dockerfile -t "movies_preprocessor:latest" .
	docker build -f ./controllers/preprocessors/ratings_preprocessor/Dockerfile -t "ratings_preprocessor:latest" .
	docker build -f ./controllers/preprocessors/credits_preprocessor/Dockerfile -t "credits_preprocessor:latest" .
	docker build -f ./controllers/filters/filter_by_country/Dockerfile -t "filter_by_country:latest" .
	docker build -f ./controllers/filters/filter_by_country_invesment/Dockerfile -t "filter_by_country_invesment:latest" .
	docker build -f ./controllers/filters/filter_by_year/Dockerfile -t "filter_by_year:latest" .
	docker build -f ./controllers/groupby/group_by_country/Dockerfile -t "group_by_country:latest" .
	docker build -f ./controllers/groupby/group_by_sentiment/Dockerfile -t "group_by_sentiment:latest" .
	docker build -f ./controllers/aggregators/aggregator_r_b/Dockerfile -t "aggregator_r_b:latest" .
	docker build -f ./controllers/aggregators/aggregator_nlp/Dockerfile -t "aggregator_nlp:latest" .
	docker build -f ./controllers/joiners/joiner_rating_by_id/Dockerfile -t "joiner_rating_by_id:latest" .
	docker build -f ./controllers/joiners/joiner_credit_by_id/Dockerfile -t "joiner_credit_by_id:latest" .
	docker build -f ./controllers/sinks/query_1/Dockerfile -t "query_1:latest" .
	docker build -f ./controllers/sinks/query_2/Dockerfile -t "query_2:latest" .
	docker build -f ./controllers/sinks/query_3/Dockerfile -t "query_3:latest" .
	docker build -f ./controllers/sinks/query_4/Dockerfile -t "query_4:latest" .
	docker build -f ./controllers/sinks/query_5/Dockerfile -t "query_5:latest" .
	docker build -f ./controllers/killer/Dockerfile -t "killer:latest" .
	docker build -f ./controllers/health_checker/Dockerfile -t "health_checker:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	@./client_spawner.sh --kill-all
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

# === Killer Management Targets ===
killer-random:
	@./manage_containers.sh --random
.PHONY: killer-random

killer-interactive:
	@./manage_containers.sh --interactive
.PHONY: killer-interactive

killer-list:
	@./manage_containers.sh --list
.PHONY: killer-list

killer-show-system:
	@./manage_containers.sh --show-system
.PHONY: killer-show-system

killer-kill:
	@read -p "Ingresa el nombre del contenedor a matar: " container && \
	./manage_containers.sh --kill $$container
.PHONY: killer-kill

killer-help:
	@./manage_containers.sh --help
.PHONY: killer-help

# === Client Management Targets ===
# client-setup:
# 	@chmod +x ./client_spawner.sh
# .PHONY: client-setup

client-spawn: #client-setup
	@read -p "Ingresa el número de clientes a iniciar: " num_clients && \
	./client_spawner.sh --spawn $$num_clients
.PHONY: client-spawn

client-view: #client-setup
	@read -p "Ingresa el número del cliente a visualizar: " client_num && \
	./client_spawner.sh --view $$client_num
.PHONY: client-view

client-list: #client-setup
	@./client_spawner.sh --list
.PHONY: client-list

client-spawn-n: #client-setup
	@./client_spawner.sh --spawn $(N)
.PHONY: client-spawn-n

client-kill: #client-setup
	@read -p "Ingresa el número del cliente a detener: " client_num && \
	./client_spawner.sh --kill $$client_num
.PHONY: client-kill

client-kill-all: #client-setup
	@./client_spawner.sh --kill-all
.PHONY: client-kill-all

