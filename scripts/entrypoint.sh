#!/bin/bash

# Wait for PostgreSQL to be ready
while ! nc -z postgres 5432; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Start uvicorn in the background
uvicorn main:app --app-dir /opt/airflow/src/moovitamix_fastapi --host 0.0.0.0 --port 8000 --reload &

# Wait a few seconds to ensure the server is up
sleep 5

# Your other commands can go here
echo "API server is running in background"
echo "Executing other tasks..."


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
