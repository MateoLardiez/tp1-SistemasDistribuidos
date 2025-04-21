FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika

COPY controllers/filters/filter_by_country_invesment /
COPY common /common
ENTRYPOINT ["/bin/sh"]