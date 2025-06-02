#!/bin/bash

N_CLIENTS=$1
N_WORKERS=$2
N_SINKERS=$3
N_NLP=$4

readonly COMPOSE_FILE="docker-compose-dev.yaml"

add_compose_header() {
    echo "name: tp1
services:" > "$COMPOSE_FILE"
}

add_rabbit_mq() {
    echo "  rabbitmq:
    container_name: rabbitmq
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    networks:
      - testing_net
    ports:
      - \"15672:15672\"
      - \"5672:5672\"
    healthcheck:
      test: [\"CMD\", \"rabbitmq-diagnostics\", \"check_port_connectivity\"]
      interval: 5s
      timeout: 5s
      retries: 5
      start_period: 5s
" >> "$COMPOSE_FILE"
}

add_gateway() {
    echo "  gateway:
    container_name: gateway
    image: gateway:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLIENTS=3
    volumes:
      - ./gateway/config.ini:/config.ini:ro
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
}

add_movies_preprocessor() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  movies_preprocessor_$i:
    container_name: movies_preprocessor_$i
    image: movies_preprocessor:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_ratings_preprocessor() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  ratings_preprocessor_$i:
    container_name: ratings_preprocessor_$i
    image: ratings_preprocessor:latest
    entrypoint: python3 /main.py
    environment:
      - N_WORKERS=$N_WORKERS
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_credits_preprocessor() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  credits_preprocessor_$i:
    container_name: credits_preprocessor_$i
    image: credits_preprocessor:latest
    entrypoint: python3 /main.py
    environment:
      - N_WORKERS=$N_WORKERS
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_filter_by_country() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  filter_by_country_$i:
    container_name: filter_by_country_$i
    image: filter_by_country:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_filter_by_country_invesment() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  filter_by_country_invesment_$i:
    container_name: filter_by_country_invesment_$i
    image: filter_by_country_invesment:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_filter_by_year() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  filter_by_year_$i:
    container_name: filter_by_year_$i
    image: filter_by_year:latest
    entrypoint: python3 /main.py
    environment:
      - N_WORKERS=$N_WORKERS
      - N_SINKERS=$N_SINKERS
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_group_by_country() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  group_by_country_$i:
    container_name: group_by_country_$i
    image: group_by_country:latest
    entrypoint: python3 /main.py
    environment:
      - N_SINKERS=$N_SINKERS
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_group_by_sentiment() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  group_by_sentiment_$i:
    container_name: group_by_sentiment_$i
    image: group_by_sentiment:latest
    entrypoint: python3 /main.py
    environment:
      - N_SINKERS=$N_SINKERS
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done  
}

add_joiner_rating_by_id() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  joiner_rating_by_id_$i:
    container_name: joiner_rating_by_id_$i
    image: joiner_rating_by_id:latest
    entrypoint: python3 /main.py
    environment:
      - N_SINKERS=$N_SINKERS
      - WORKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_joiner_credit_by_id() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  joiner_credit_by_id_$i:
    container_name: joiner_credit_by_id_$i
    image: joiner_credit_by_id:latest
    entrypoint: python3 /main.py
    environment:
      - N_SINKERS=$N_SINKERS
      - WORKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}


add_aggregator_nlp() {
  for ((i=0; i<=N_NLP-1; i++)); do
    echo "  aggregator_nlp_$i:
    container_name: aggregator_nlp_$i
    image: aggregator_nlp:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}

add_aggregator_r_b() {
  for ((i=0; i<=N_WORKERS-1; i++)); do
    echo "  aggregator_r_b_$i:
    container_name: aggregator_r_b_$i
    image: aggregator_r_b:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
  done
}


add_sinker_q1() {
  for ((i=0; i<=N_SINKERS-1; i++)); do
    echo "  query_1_sinker_$i:
    container_name: query_1_sinker_$i
    image: query_1:latest
    entrypoint: python3 /main.py
    environment:
      - SINKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
  done
}

add_sinker_q2() {
  for ((i=0; i<=N_SINKERS-1; i++)); do
    echo "  query_2_sinker_$i:
    container_name: query_2_sinker_$i
    image: query_2:latest
    entrypoint: python3 /main.py
    environment:
      - SINKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
  done
}

add_sinker_q3() {
  for ((i=0; i<=N_SINKERS-1; i++)); do
    echo "  query_3_sinker_$i:
    container_name: query_3_sinker_$i
    image: query_3:latest
    entrypoint: python3 /main.py
    environment:
      - N_WORKERS=$N_WORKERS
      - SINKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
  done
}

add_sinker_q4() {
  for ((i=0; i<=N_SINKERS-1; i++)); do
    echo "  query_4_sinker_$i:
    container_name: query_4_sinker_$i
    image: query_4:latest
    entrypoint: python3 /main.py
    environment:
      - N_WORKERS=$N_WORKERS
      - SINKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
  done
}

add_sinker_q5() {
  for ((i=0; i<=N_SINKERS-1; i++)); do
    echo "  query_5_sinker_$i:
    container_name: query_5_sinker_$i
    image: query_5:latest
    entrypoint: python3 /main.py
    environment:
      - SINKER_ID=$i
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
  done
}

add_results_tester() {
  echo "  results_tester:
    container_name: results_tester
    image: results_tester:latest
    entrypoint: python3 /main.py
    volumes:
      - ./.data/results.json:/results.json:ro
    networks:
      - testing_net
" >> "$COMPOSE_FILE"
}

add_killer() {
  echo "  killer:
    container_name: killer
    image: killer:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    stdin_open: true
    tty: true
    networks:
      - testing_net
    entrypoint: [\"/bin/sh\"]
    # Para usar en modo interactivo:
    # docker exec -it killer python main.py --interactive
    # Para matar un contenedor específico:
    # docker exec killer python main.py --kill <container_name>
" >> "$COMPOSE_FILE"
}

add_client() {
  for ((i=0; i<=N_CLIENTS-1; i++)); do
    echo "  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$((i+1))
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./.data/movies_metadata.csv:/movies.csv:ro
      - ./.data/ratings.csv:/ratings.csv:ro
      - ./.data/credits.csv:/credits.csv:ro
    networks:
      - testing_net
    depends_on:
      gateway:
        condition: service_started
  " >> "$COMPOSE_FILE"
  done
}

add_networks() {
    echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> "$COMPOSE_FILE"

}

check_params() {
    if [ -z "$N_WORKERS" ]; then
        echo "Usage: $0 <number_of_workers>"
        exit 1
    fi

    if [ -z "$N_CLIENTS" ]; then
        echo "Usage: $0 <number_of_workers> <number_of_clients>"
        exit 1
    fi

    if [ -z "$N_SINKERS" ]; then
        echo "Usage: $0 <number_of_workers> <number_of_clients> <number_of_sinkers>"
        exit 1
    fi

    if ! [[ "$N_CLIENTS" =~ ^[0-9]+$ ]]; then
        echo "Error: <number_of_clients> must be a positive integer."
        exit 1
    fi

    if ! [[ "$N_WORKERS" =~ ^[0-9]+$ ]]; then
        echo "Error: <number_of_workers> must be a positive integer."
        exit 1
    fi

    if ! [[ "$N_SINKERS" =~ ^[0-9]+$ ]]; then
        echo "Error: <number_of_sinkers> must be a positive integer."
        exit 1
    fi

    if ! [[ "$N_NLP" =~ ^[0-9]+$ ]]; then
        echo "Error: <number_of_nlp> must be a positive integer."
        exit 1
    fi

    if [ "$N_SINKERS" -lt 1 ]; then
        echo "Error: <number_of_sinkers> must be at least 1."
        exit 1
    fi

    if [ "$N_WORKERS" -lt 1 ]; then
        echo "Error: <number_of_workers> must be at least 1."
        exit 1
    fi

    if [ "$N_CLIENTS" -lt 1 ]; then
        echo "Error: <number_of_clients> must be at least 1."
        exit 1
    fi

    if [ "$N_NLP" -lt 1 ]; then
        echo "Error: <number_of_nlp> must be at least 1."
        exit 1
    fi
}

# ---------------------------------------- #

check_params
add_compose_header
add_rabbit_mq
add_gateway
add_movies_preprocessor
add_ratings_preprocessor
add_credits_preprocessor
add_filter_by_country
add_filter_by_country_invesment
add_filter_by_year
add_group_by_country
add_group_by_sentiment
add_joiner_rating_by_id
add_joiner_credit_by_id
add_aggregator_nlp
add_aggregator_r_b
add_sinker_q1
add_sinker_q2
add_sinker_q3
add_sinker_q4
add_sinker_q5
add_results_tester
add_killer
add_client
add_networks