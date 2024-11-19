#!/bin/bash

DEFAULT_PORT=8080
DEFAULT_HOST="localhost"
DEFAULT_MAX_RETRIES=10
DEFAULT_RETRY_INTERVAL=5

check_port() {
    local port=${1:-$DEFAULT_PORT}
    local host=${2:-$DEFAULT_HOST}
    local max_retries=${3:-$DEFAULT_MAX_RETRIES}
    local retry_interval=${4:-$DEFAULT_RETRY_INTERVAL}
    
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        echo "Error: Port must be a number"
        echo "Usage: $0 <port> [host] [max_retries] [retry_interval]"
        return 1
    fi
    echo "Checking port $port on host $host (max retries: $max_retries, interval: ${retry_interval}s)"
    
    local retries=0
    while [ $retries -lt $max_retries ]; do
        nc -z $host $port
        if [ $? -eq 0 ]; then
            echo "✅ Port $port is running on $host"
            return 0
        fi
        echo "Attempt $((retries+1))/$max_retries - Waiting for port $port on $host..."
        sleep $retry_interval
        retries=$((retries+1))
    done
    
    echo "❌ Timeout - Port $port not available on $host after $max_retries attempts"
    return 1
}

# Start uvicorn in the background
echo "Starting uvicorn server..."
uvicorn main:app --app-dir /opt/airflow/src/moovitamix_fastapi --host 0.0.0.0 --port 8000 --reload &

# Checking if api is up (exemplos de uso)
# Exemplo 1: usando localhost
if ! check_port 8000 "localhost"; then
    echo "Failed to start uvicorn server"
    exit 1
fi

if ! check_port 5432 "postgres" 20 30; then
    echo "Failed to start Postgres server"
    exit 1
fi

# Initialize Airflow DB if it hasn't been initialized
echo "Initializing Airflow DB..."
NEW_KEY_FERNET=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")
export AIRFLOW__CORE__FERNET_KEY=$NEW_KEY_FERNET
airflow db init

# Create admin user if it doesn't exist
echo "Creating admin user..."
airflow users list | grep -q 'admin' || (
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname Admin \
        --role Admin \
        --email admin@admin.com \
        --password admin
)

# Start Airflow webserver and scheduler
echo "Starting Airflow webserver and scheduler..."
airflow webserver & airflow scheduler
