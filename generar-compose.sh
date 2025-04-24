#!/bin/bash

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
    echo "  movies_preprocessor:
    container_name: movies_preprocessor
    image: movies_preprocessor:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"

}

add_ratings_preprocessor() {
    echo "  ratings_preprocessor:
    container_name: ratings_preprocessor
    image: ratings_preprocessor:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"

}

add_credits_preprocessor() {
    echo "  credits_preprocessor:
    container_name: credits_preprocessor
    image: credits_preprocessor:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"

}

add_filter_by_country() {
    echo "  filter_by_country:
    container_name: filter_by_country
    image: filter_by_country:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"

}

add_filter_by_country_invesment() {
    echo "  filter_by_country_invesment:
    container_name: filter_by_country_invesment
    image: filter_by_country_invesment:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"

}

add_filter_by_year() {
    echo "  filter_by_year:
    container_name: filter_by_year
    image: filter_by_year:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
}

add_joiner_rating_by_id() {
    echo "  joiner_rating_by_id:
    container_name: joiner_rating_by_id
    image: joiner_rating_by_id:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
}

add_aggregator_nlp() {
    echo "  aggregator_nlp:
    container_name: aggregator_nlp
    image: aggregator_nlp:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
}

add_aggregator_r_b() {
    echo "  aggregator_r_b:
    container_name: aggregator_r_b
    image: aggregator_r_b:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
" >> "$COMPOSE_FILE"
}


add_sinker_q1() {
  echo "  query_1:
    container_name: query_1
    image: query_1:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
}

add_sinker_q2() {
  echo "  query_2:
    container_name: query_2
    image: query_2:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
}

add_sinker_q5() {
  echo "  query_5:
    container_name: query_5
    image: query_5:latest
    entrypoint: python3 /main.py
    networks:
      - testing_net
    depends_on:
      rabbitmq:
        condition: service_healthy
  " >> "$COMPOSE_FILE"
}

add_client() {
    echo "  client1:
    container_name: client1
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=1
    volumes:
      - ./client/config.yaml:/config.yaml:ro
      - ./.data/movies_sample.csv:/movies.csv:ro
      - ./.data/ratings_sample.csv:/ratings.csv:ro
      - ./.data/credits_sample.csv:/credits.csv:ro
    networks:
      - testing_net
    depends_on:
      gateway:
        condition: service_started
" >> "$COMPOSE_FILE"

}

add_networks() {
    echo "networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24" >> "$COMPOSE_FILE"

}

# ---------------------------------------- #

add_compose_header
add_rabbit_mq
add_gateway
add_movies_preprocessor
add_ratings_preprocessor
add_credits_preprocessor
add_filter_by_country
add_filter_by_country_invesment
add_filter_by_year
add_joiner_rating_by_id
add_aggregator_nlp
add_aggregator_r_b
add_sinker_q1
add_sinker_q2
add_sinker_q5
add_client
add_networks