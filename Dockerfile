# Use a lightweight Python base image
FROM python:3.10-slim

# Set environment variables
# 1. Force Python stdout to be unbuffered (logs show up immediately)
# 2. Prevent Python from creating .pyc files
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set work directory
WORKDIR /app

# Install system dependencies (gcc required for uvloop/pandas compilation sometimes)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create log/data files to prevent permission issues
RUN touch bot_execution.log active_trades.json trade_history.csv

# Run the bot
CMD ["python", "main.py"]