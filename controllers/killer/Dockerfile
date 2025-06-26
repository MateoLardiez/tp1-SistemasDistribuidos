FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker

COPY controllers/killer /
COPY common /common
ENTRYPOINT ["/bin/sh"]