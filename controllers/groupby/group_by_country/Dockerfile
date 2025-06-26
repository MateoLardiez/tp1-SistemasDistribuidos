FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/groupby/group_by_country /
COPY common /common
ENTRYPOINT ["/bin/sh"]