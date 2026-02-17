FROM python:3.12-slim

WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt web/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r web/requirements.txt

# Copy application code
COPY . .

# Create data directory for state DB and logs
RUN mkdir -p /app/data

# Environment defaults
ENV PORT=8000
ENV HOST=0.0.0.0
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Run the web app
CMD ["python", "-m", "web.app"]
