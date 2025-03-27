#!/bin/bash
set -e

TEMPLATE_FILE="/etc/redis/sentinel.conf.template"
CONFIG_FILE="/etc/redis/sentinel.conf"
ENV_DIR="/env"

# Start fresh from the template
cp "$TEMPLATE_FILE" "$CONFIG_FILE"

# Loop over all .env files in /env
for env_file in "$ENV_DIR"/*.env; do
    echo "Processing $env_file..."
    
    # Source the env vars
    set -a
    source "$env_file"
    set +a

    # Derive a service name from the filename (e.g., "order" from "order_redis.env")
    SERVICE_NAME=$(basename "$env_file" | cut -d'_' -f1)

    # Append Sentinel directives for each Redis master
    {
        echo ""
        echo "# Monitor $SERVICE_NAME Redis master"
        echo "sentinel monitor $SERVICE_NAME ${REDIS_MASTER_HOST} ${REDIS_MASTER_PORT} 1"
        echo "sentinel auth-pass $SERVICE_NAME ${REDIS_PASSWORD}"
        echo "sentinel down-after-milliseconds $SERVICE_NAME 1000"
        echo "sentinel failover-timeout $SERVICE_NAME 1000"
    } >> "$CONFIG_FILE"
done

echo "Generated sentinel.conf:"
cat "$CONFIG_FILE"

# Start Sentinel
exec "$@"