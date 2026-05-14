# =========================================================
# Autonomous Engineering Runtime
# Dockerfile
# =========================================================

FROM python:3.12-slim

# Prevent Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Ensure stdout/stderr are unbuffered
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Create runtime directories
RUN mkdir -p \
    runtime_data \
    logs

# Expose optional runtime port
EXPOSE 8000

# Default runtime environment
ENV RUNTIME_ENV=container

# Default command
CMD ["python", "-m", "autonomous_runtime"]