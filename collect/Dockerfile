# Use the official Apache Spark image as a base
FROM apache/spark-py

USER root

# Install necessary packages
COPY requirements.txt /requirements.txt
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install --no-cache-dir -r /requirements.txt

# Copy the main into the image
COPY main.py /main.py

ARG CURRENT_IP

ENV MINIO_HOST=$CURRENT_IP

# Set the entry point to run the main
ENTRYPOINT ["python3", "/main.py"]
