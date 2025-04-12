# Use Python 3.9 base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Prevents Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# Prevents Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# Install system dependencies (may be needed for Firebase or XGBoost)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libglib2.0-0 libsm6 libxext6 libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy model, Firebase credentials, and app code
COPY . .

# Start FastAPI with Uvicorn on port 8080 (Fly.io expects this)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
