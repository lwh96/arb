#!/bin/bash

# A simple script to install Docker and run the bot on Ubuntu 22.04

echo "Setting up Docker Environment..."

# 1. Install Docker Engine
# Remove old versions
apt-get remove -y docker docker-engine docker.io containerd runc

# Update apt and install prerequisites
apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# 2. Build and Run the Bot
echo "Building and Starting Bot Container..."

# Ensure .env exists (Create dummy if not, you MUST edit this later)
if [ ! -f .env ]; then
    echo ".env file not found! Creating a placeholder. Please edit it with your keys."
    touch .env
fi

# Ensure data files exist on host to mount properly
touch bot_execution.log active_trades.json trade_history.csv

# Build and Run in background (-d)
docker compose up -d --build

echo "Bot is running in Docker!"
echo "View logs with: docker compose logs -f"