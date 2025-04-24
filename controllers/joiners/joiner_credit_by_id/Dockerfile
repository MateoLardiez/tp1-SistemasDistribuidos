FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/joiners/joiner_credit_by_id /
COPY common /common
ENTRYPOINT ["/bin/sh"]