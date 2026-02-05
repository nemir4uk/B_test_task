FROM python:3.12-alpine

COPY .env /
COPY requirements.txt /
COPY src/ /src/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /src


CMD ["python", "consumer.py"]