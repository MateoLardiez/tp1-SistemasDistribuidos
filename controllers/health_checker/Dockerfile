FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install docker

COPY controllers/health_checker /
COPY common /common
ENTRYPOINT ["/bin/sh"]