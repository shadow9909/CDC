# Change Data Capture (CDC) Demo with Debezium, Kafka, and PostgreSQL

## Overview

This project demonstrates a **Change Data Capture (CDC)** pipeline using:

- **PostgreSQL** (as source and target databases)
- **Debezium** (for streaming database changes)
- **Kafka** (as the event broker)
- **Python Consumer** (to sync changes to the target DB)

It is fully containerized using Docker Compose for easy local development and testing.

---

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│   Source DB     │    │   Debezium   │    │    Kafka    │    │   Consumer   │
│ (Postgres)      │    │  (Connect)   │    │  (Broker)   │    │  (Python)    │
│ wal_level=      │───▶│ Reads WAL    │───▶│ Topics:     │───▶│ Processes    │
│ logical         │    │ via logical  │    │ - users     │    │ & writes to  │
│                 │    │ replication  │    │ - orders    │    │ Target DB    │
└─────────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
                                                   ▲
                                                   │
                                           ┌──────────────┐
                                           │  Zookeeper   │
                                           │ (Kafka mgmt) │
                                           └──────────────┘
```

---

## Components

### 1. Source PostgreSQL Database
- Runs on port **5432**
- Contains `users` and `orders` tables
- Initialized with sample data (see `init-scripts/source-db-init.sql`)
- Configured for logical replication (`wal_level=logical`)

### 2. Target PostgreSQL Database
- Runs on port **5433**
- Same schema as source, with an extra `synced_at` column
- Receives changes from the consumer

### 3. Zookeeper & Kafka
- **Zookeeper** manages Kafka cluster state
- **Kafka** (port **9092** for host, **29092** for internal Docker)
- Stores change events as topics (e.g., `dbserver1.public.users`)

### 4. Debezium Connect
- Streams changes from source Postgres to Kafka topics
- Configured via `debezium-config/postgres-connector.json`
- REST API on port **8083**

### 5. Python Consumer
- Reads change events from Kafka
- Applies inserts/updates/deletes to the target Postgres
- Code in `consumer/consumer.py`
- Requirements in `consumer/requirements.txt`

---

## Quick Start

### Prerequisites
- [Docker](https://www.docker.com/products/docker-desktop)
- [Python 3.8+](https://www.python.org/downloads/)

### 1. Clone the Repository
```bash
git clone <this-repo-url>
cd <repo-root>
```

### 2. Start All Services
```bash
./setup.sh
```
- This script:
  - Starts all containers
  - Waits for services to be ready
  - Registers the Debezium connector

### 3. Start the Python Consumer
In a new terminal:
```bash
cd consumer
pip install -r requirements.txt
python consumer.py
```

### 4. Test the CDC Pipeline
- Use the provided test script to simulate changes:

```bash
python ../test.py
```
- This will:
  - Insert, update, and delete records in the source DB
  - Show how changes propagate to the target DB

---

## File/Directory Structure

- `docker-compose.yaml` — Orchestrates all services
- `setup.sh` — One-click setup for containers and Debezium connector
- `init-scripts/` — SQL scripts to initialize source/target databases
- `debezium-config/` — Debezium connector configuration
- `consumer/` — Python CDC consumer and debug tools
- `test.py` — Step-by-step CDC test script
- `check_target_db.py` — Utility to inspect target DB contents

---

## Useful Scripts & Tools

- **consumer/debug_messages.py** — Inspect raw Kafka messages
- **consumer/debug_kafka.py** — List topics, check Debezium status, peek at messages
- **check_target_db.py** — Print all data in the target DB

---

## Troubleshooting

- **Kafka connection errors?**
  - Ensure Kafka is running (`docker ps`)
  - Check that the consumer is using `localhost:9092`
- **No data in target DB?**
  - Check consumer logs for errors
  - Ensure Debezium connector is running (`curl http://localhost:8083/connectors`)
- **Resetting the pipeline:**
  - `docker-compose down -v` (removes all data)
  - `./setup.sh` to restart

---

## Customization

- **Change tables:** Edit `table.include.list` in `debezium-config/postgres-connector.json`
- **Change initial data:** Edit SQL in `init-scripts/`
- **Add more consumers:** Duplicate and modify `consumer/consumer.py`

---

## References
- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/en/master/)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

---

## License
MIT 