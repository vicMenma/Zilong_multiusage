FROM python:3.11-slim

# Install system dependencies: ffmpeg, aria2, and build tools
RUN apt-get update && apt-get install -y \
    ffmpeg \
    aria2 \
    git \
    curl \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the bot code
COPY . .

# Create the downloads/temp directory (will be mounted as a volume)
RUN mkdir -p /app/downloads

# Expose port for webhook
EXPOSE 8080

# Start the bot
CMD ["python", "main.py"]
