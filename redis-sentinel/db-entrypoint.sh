#!/bin/bash
set -e

# Load environment from file if provided
if [[ -f "/env/redis.env" ]]; then
    echo "[INIT] Sourcing environment from /env/redis.env"
    set -a
    source /env/redis.env
    set +a
else
    echo "[ERROR] /env/redis.env not found. Please mount the env file as /env/redis.env"
    exit 1
fi

sleep 5

IFS=',' read -ra SENTINELS <<< "$REDIS_SENTINEL_HOSTS"
SENTINEL_PORT="${REDIS_SENTINEL_PORT:-26379}"
REDIS_SERVICE_NAME="${REDIS_SERVICE_NAME:?Missing REDIS_SERVICE_NAME}"
REDIS_PASSWORD="${REDIS_PASSWORD:?Missing REDIS_PASSWORD}"
REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-512mb}"

SENTINEL_HOST="${SENTINELS[0]}"

# echo "[INIT] Querying Sentinel at $SENTINEL_HOST:$SENTINEL_PORT for master of '$REDIS_SERVICE_NAME'..."

# Get the current master for the Redis service
# read -r MASTER_HOST MASTER_PORT <<< $(redis-cli -h "$SENTINEL_HOST" -p "$SENTINEL_PORT" SENTINEL get-master-addr-by-name "$REDIS_SERVICE_NAME" | xargs)
# MAX_RETRIES=10
# RETRY_DELAY=2
# for attempt in $(seq 1 $MAX_RETRIES); do
#     echo "[DEBUG] redis-cli -h $SENTINEL_HOST -p $SENTINEL_PORT SENTINEL get-master-addr-by-name $REDIS_SERVICE_NAME"
#     set -x
#     redis-cli -h "$SENTINEL_HOST" -p "$SENTINEL_PORT" SENTINEL get-master-addr-by-name "$REDIS_SERVICE_NAME"
#     set +x
#     RAW_MASTER=$(redis-cli -h "$SENTINEL_HOST" -p "$SENTINEL_PORT" SENTINEL get-master-addr-by-name "$REDIS_SERVICE_NAME" | xargs)
#     echo "[DEBUG] RAW_MASTER: $RAW_MASTER"
#     if [[ -n "$RAW_MASTER" ]]; then
#         read -r MASTER_HOST MASTER_PORT <<< "$(echo $RAW_MASTER | xargs)"
#         if [[ -n "$MASTER_HOST" && -n "$MASTER_PORT" ]]; then
#             break
#         fi
#     fi
#     echo "[WAIT] Sentinel not ready yet. Attempt $attempt/$MAX_RETRIES..."
#     sleep "$RETRY_DELAY"
# done

# if [[ -z "$MASTER_HOST" || -z "$MASTER_PORT" ]]; then
#   echo "[FAIL] Could not get master info after $MAX_RETRIES retries."
#   exit 1
# fi

# MY_IP=$(hostname -i)

# echo "[INFO] My IP: $MY_IP"
# echo "[INFO] Sentinel reports current master: $MASTER_HOST:$MASTER_PORT"

# if [[ "$MASTER_HOST" != "$MY_IP" ]]; then
MASTER_HOST="${REDIS_MASTER_HOST}"
MASTER_PORT="${REDIS_MASTER_PORT}"
echo "[ROLE] Starting as replica of $MASTER_HOST:$MASTER_PORT"
exec redis-server --requirepass "$REDIS_PASSWORD" --masterauth "$REDIS_PASSWORD" --replicaof "$MASTER_HOST" "$MASTER_PORT" --maxmemory "$REDIS_MAXMEMORY"
# else
#     echo "[ROLE] I am the current master. Starting in master mode."
#     exec redis-server \
#         --requirepass "$REDIS_PASSWORD" \
#         --masterauth "$REDIS_PASSWORD" \
#         --maxmemory "$REDIS_MAXMEMORY"
# fi
