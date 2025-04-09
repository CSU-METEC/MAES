FROM python:3.11.3-slim-buster

WORKDIR /maes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYDEVD_DISABLE_FILE_VALIDATION=1 \
    # Turns off buffering for easier container logging
    PYTHONUNBUFFERED=1

COPY . .

RUN pip install --upgrade pip

RUN pip install poetry 

RUN poetry config virtualenvs.create false

# Set Python path to include MAES source
ENV PYTHONPATH="/maes/src:${PYTHONPATH}"

# Install all dependencies of the current project
RUN poetry install