# Adnomaly Quick Start Guide

## What is Adnomaly?
Adnomaly is a real-time clickstream analytics platform that detects ad fraud anomalies. It generates fake clickstream data and stores it permanently so you never lose your data.

## What You Need
- **Docker** (with Docker Compose)
- **Python 3.11+**
- **Make** (usually comes with macOS/Linux)

## Quick Start (3 Steps)

### Step 1: Start Everything
```bash
make up
```
This starts:
- Kafka (for streaming data)
- PostgreSQL (stores data permanently)
- MinIO (data lake storage)
- Flink (for processing)

### Step 2: Generate Data
```bash
make gen
```
This creates fake clickstream events and sends them to Kafka.

### Step 3: Store Data (Choose One)
```bash
# Store in database (recommended)
make db-consumer

# OR store in data lake
make minio-consumer

# OR just view data (no storage)
make tail
```

## What's Happening?

1. **Data Generation**: Fake clickstream events are created (like real ad clicks)
2. **Data Storage**: Events are saved permanently in:
   - **PostgreSQL**: For quick queries and analytics
   - **MinIO**: For long-term data lake storage
3. **Data Persistence**: Your data survives restarts and system reboots

## Check Your Data

### View Database Data
```bash
make db-query
```
Shows:
- Total events stored
- Recent events
- Statistics by country/platform

### Access MinIO Console
1. Open browser: http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Browse your data in the `adnomaly-data` bucket

## Stop Everything
```bash
make down
```

## Advanced Usage

### Generate More Data
```bash
# Generate 50 events per second
EVENTS_PER_SEC=50 make gen
```

### Run Multiple Consumers
```bash
# Terminal 1: Generate data
make gen

# Terminal 2: Store in database
make db-consumer

# Terminal 3: Store in data lake
make minio-consumer
```

### Test Everything
```bash
make test
```

## Troubleshooting

### Port Already in Use?
If you get port errors:
- PostgreSQL uses port 5433 (not 5432)
- MinIO uses ports 9000 and 9001
- Kafka uses port 29092

### Data Not Appearing?
1. Make sure containers are running: `docker ps`
2. Check if topics exist: `make create-topic`
3. Restart consumers if needed

### Reset Everything
```bash
make down
make up
make create-topic
```

## What's Stored?

### PostgreSQL Database
- **clickstream_events**: All your clickstream data
- **feature_aggregates**: Processed analytics
- **anomaly_results**: Fraud detection results

### MinIO Data Lake
- JSON files organized by date/hour
- Perfect for big data analysis
- Survives system restarts

## Data Flow
```
Generator → Kafka → PostgreSQL + MinIO + Console Output
```

Your data is now **permanently stored** and will survive any system restarts! 🎉

## Need Help?
- Check the logs: `docker logs <container-name>`
- View the full documentation: `TESTING_SUMMARY.md`
- All data is persistent - you won't lose anything
