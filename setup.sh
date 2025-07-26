#!/bin/bash

echo "Starting CDC system setup..."

# Start services
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 30

# Register Debezium connector
echo "Registering Debezium connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-config/postgres-connector.json

echo "Setup complete! Check connector status:"
curl http://localhost:8083/connectors/postgres-source-connector/status
