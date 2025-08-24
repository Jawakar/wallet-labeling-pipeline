FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container
COPY extract_ethereum_transactions.py .
COPY schemas.py .

# Create directory for credentials
RUN mkdir -p /app/credentials && \
    chmod 755 /app/credentials

# Command to run when the container starts
CMD ["faust", "-A", "extract_ethereum_transactions", "worker", "-l", "info"]