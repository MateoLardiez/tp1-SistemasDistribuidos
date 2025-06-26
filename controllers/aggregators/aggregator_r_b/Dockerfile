FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/aggregators/aggregator_r_b /
COPY common /common
ENTRYPOINT ["/bin/sh"]