FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY src/ /src/
COPY alembic/ /alembic/
COPY *.ini /

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt



