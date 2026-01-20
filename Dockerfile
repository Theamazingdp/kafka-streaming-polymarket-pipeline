FROM python:3.11-slim

#Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts from producers and consumers directories
COPY producers/*.py .
COPY consumers/*.py .

# Default command (will be overridden in docker-compose)
CMD ["python", "-u"]