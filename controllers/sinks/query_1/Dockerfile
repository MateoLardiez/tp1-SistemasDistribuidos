FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/sinks/query_1 /
COPY common /common
ENTRYPOINT ["/bin/sh"]