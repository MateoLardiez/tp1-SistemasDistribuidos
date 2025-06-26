FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/preprocessors/movies_preprocessor /
COPY common /common
ENTRYPOINT ["/bin/sh"]