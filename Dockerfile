# Use Python 3.9
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies first (cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Then copy the rest (model, code)
COPY . .

# Run FastAPI on port 8080 (Fly.io expects this)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
