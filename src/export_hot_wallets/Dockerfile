# Base image with Spark and Python
FROM bitnami/spark:3.5.0

USER root

# Install native dependencies required by RocksDB
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        librocksdb-dev \
        libsnappy-dev \
        zlib1g-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory inside the container
WORKDIR /opt/spark-app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script(s) into the container
COPY export_hot_wallets.py schemas.py /opt/spark-app/jobs/

# Create and set permissions for the checkpoint directory
RUN mkdir -p /opt/spark-app/checkpoints && \
    chown -R 1001:1001 /opt/spark-app/checkpoints

USER 1001

# Command to run the Spark job script
CMD ["python", "/opt/spark-app/jobs/export_hot_wallets.py"]