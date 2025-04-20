FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY gateway /
COPY common /common
ENTRYPOINT ["/bin/sh"]
