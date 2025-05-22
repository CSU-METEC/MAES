FROM python:3.11.3-slim-buster

WORKDIR /maes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYDEVD_DISABLE_FILE_VALIDATION=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

COPY . .

RUN pip install --upgrade pip

RUN pip install "./[docker]"

# Set Python path to include MAES source
ENV PYTHONPATH="/maes/src"

