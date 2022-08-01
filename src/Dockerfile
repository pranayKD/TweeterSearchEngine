FROM python:2.7
WORKDIR /twitter_project/

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    RABBITMQ_LOCALHOST=rabbitmq \
    MONGO_LOCALHOST=mongo \
    ES_LOCALHOST=es


COPY requirements.txt /twitter_project/
RUN pip install -r requirements.txt

COPY .  /twitter_project/

