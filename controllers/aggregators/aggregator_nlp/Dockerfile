FROM python:3.11-slim
RUN pip install --upgrade pip && pip3 install pika && pip install transformers torch && pip install hf_xet

COPY controllers/aggregators/aggregator_nlp /
COPY common /common
ENTRYPOINT ["/bin/sh"]