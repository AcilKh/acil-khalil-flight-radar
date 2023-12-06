FROM python:3.10

WORKDIR /app

COPY requirements.txt .

COPY airlines.json .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ARG CURRENT_IP
ENV MINIO_HOST=$CURRENT_IP

CMD ["python", "main.py"]

