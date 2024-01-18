#!/bin/bash

# Start Redis in the background
redis-server &

# Function to check if Redis is ready
redis_ready() {
  redis-cli ping >/dev/null 2>&1
}

# Wait for Redis to start up
until redis_ready; do
  echo "Waiting for Redis to start..."
  sleep 1
done

# Load the Lua script into Redis
cat /usr/local/bin/ermeslib.lua | redis-cli -x FUNCTION LOAD REPLACE

# Keep the script running to keep the container alive
tail -f /dev/null
