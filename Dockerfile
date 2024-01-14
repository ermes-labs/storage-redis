# Use the official Redis image as a parent image
FROM redis

# Copy the Lua script and the custom entrypoint script into the container
COPY ermeslib.lua /usr/local/bin/ermeslib.lua
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Give execution rights on the entrypoint script
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the custom entrypoint script as the entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
