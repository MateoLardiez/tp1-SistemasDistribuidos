FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY gateway /
RUN python -m unittest tests/test_common.py
ENTRYPOINT ["/bin/sh"]
