FROM rabbitmq:4.1-management-alpine
RUN apk update && apk add curl

RUN mkdir -p /etc/rabbitmq/conf.d
RUN chown -R rabbitmq:rabbitmq /etc/rabbitmq/conf.d
COPY config.conf /etc/rabbitmq/conf.d