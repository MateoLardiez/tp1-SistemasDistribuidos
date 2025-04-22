FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/aggregator/aggregator_r_b /
COPY common /common
ENTRYPOINT ["/bin/sh"]