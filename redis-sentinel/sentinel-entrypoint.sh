#!/bin/bash
set -e

TEMPLATE_FILE="/etc/redis/sentinel.conf.template"
CONFIG_FILE="/etc/redis/sentinel.conf"
ENV_DIR="/env"

# Start fresh from the template
cp "$TEMPLATE_FILE" "$CONFIG_FILE"

# {
#     echo ""
#     echo "sentinel announce-ip ${SENTINEL_IP}"
# } >> "$CONFIG_FILE"

# Loop over all .env files in /env
for env_file in "$ENV_DIR"/*.env; do
    echo "Processing $env_file..."
    
    # Source the env vars
    set -a
    source "$env_file"
    set +a

    # Append Sentinel directives for each Redis master
    {
        echo ""
        echo "sentinel monitor ${REDIS_SERVICE_NAME} ${REDIS_MASTER_HOST} ${REDIS_MASTER_PORT} 1"
        echo "sentinel auth-pass ${REDIS_SERVICE_NAME} ${REDIS_PASSWORD}"
        echo "sentinel down-after-milliseconds ${REDIS_SERVICE_NAME} 1000"
        echo "sentinel failover-timeout ${REDIS_SERVICE_NAME} 1000"
        echo "sentinel announce-ip ${SENTINEL_IP}"
        echo "sentinel announce-port ${SENTINEL_PORT}"
    } >> "$CONFIG_FILE"
done

echo "Generated sentinel.conf:"
cat "$CONFIG_FILE"

# Start Sentinel
exec redis-server "$CONFIG_FILE" --sentinel --port "$SENTINEL_PORT"