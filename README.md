# Adnomaly - Phase 1: Local Playground (MVP Data Flow)

A real-time clickstream analytics platform for detecting ad fraud anomalies. This is Phase 1 implementation focusing on local development with Kafka-based data flow.

## Prerequisites

- **Docker** (with Docker Compose)
- **Python 3.11+**
- **Make** (for running commands)

## Quickstart

1. **Start Kafka infrastructure:**
   ```bash
   make up
   ```

2. **Create the clickstream topic:**
   ```bash
   make create-topic
   ```

3. **Generate clickstream events:**
   ```bash
   make gen
   ```

4. **In another terminal, consume events:**
   ```bash
   make tail
   ```

## Project Structure

```
.
├── infra/
│   └── docker-compose.yml      # Kafka + Zookeeper setup
├── data/
│   ├── generator.py            # Event producer
│   └── schema.py              # Pydantic schema validation
├── consumers/
│   └── tail.py                # Event consumer
├── tests/
│   ├── test_schema.py         # Schema validation tests
│   └── test_smoke.py          # Generator smoke tests
├── requirements.txt           # Pinned Python dependencies
├── Makefile                   # Build and run commands
└── README.md                  # This file
```

## Environment Variables

### Producer (data/generator.py)
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `localhost:29092` | Kafka bootstrap servers |
| `TOPIC` | `clickstream` | Target Kafka topic |
| `EVENTS_PER_SEC` | `120` | Events generated per second |
| `HASH_SALT` | `salt` | Salt for user ID hashing |

### Consumer (consumers/tail.py)
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `localhost:29092` | Kafka bootstrap servers |
| `TOPIC` | `clickstream` | Source Kafka topic |
| `GROUP_ID` | `tail-cli` | Consumer group ID |
| `AUTO_OFFSET_RESET` | `latest` | Offset reset policy (`earliest`/`latest`) |

## Event Schema

Each clickstream event contains the following fields:

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "user_id_hash": "a1b2c3d4e5f67890",
  "ad_id": "ad_1234",
  "campaign_id": "camp_567",
  "geo": "US",
  "platform": "web",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  "CPC": 0.75,
  "CTR": 0.025,
  "conversion": 0,
  "bounce_rate": 0.45
}
```

### Field Constraints

- **timestamp**: ISO8601 UTC format ending with 'Z'
- **user_id_hash**: 16-64 hex characters (SHA-256 hash)
- **ad_id**: Format `ad_<digits>`
- **campaign_id**: Format `camp_<digits>`
- **geo**: Exactly 2 uppercase letters (US, IN, BR, DE, JP)
- **platform**: One of "web", "ios", "android"
- **user_agent**: Non-empty string
- **CPC**: Float ≥ 0 (typically 0.05-2.0)
- **CTR**: Float 0.0-1.0 (typically 0.005-0.10)
- **conversion**: Integer 0 or 1
- **bounce_rate**: Float 0.0-1.0 (typically 0.2-0.9)

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make up` | Start Kafka and Zookeeper containers |
| `make down` | Stop and remove containers with volumes |
| `make create-topic` | Create clickstream topic (3 partitions) |
| `make gen` | Run event generator (120 events/sec) |
| `make tail` | Run event consumer (prints to stdout) |
| `make test` | Run all tests |

## Testing

Run the test suite:
```bash
make test
```

Tests cover:
- Schema validation (valid/invalid events)
- Event generation (structure and constraints)
- Field format validation
- Data type validation

## Troubleshooting

### Kafka Connection Issues

1. **Connection refused**: Ensure Kafka is running
   ```bash
   make up
   # Wait for containers to be healthy
   ```

2. **Topic not found**: Create the topic
   ```bash
   make create-topic
   ```

3. **Consumer not receiving messages**: Check offset reset
   ```bash
   AUTO_OFFSET_RESET=earliest make tail
   ```

### Common Issues

- **Port conflicts**: Ensure ports 2181 (Zookeeper) and 29092 (Kafka) are available
- **Python dependencies**: Run `pip install -r requirements.txt` in virtual environment
- **Permission errors**: Ensure Docker has proper permissions

### Replaying Events

To replay from the beginning of the topic:
```bash
AUTO_OFFSET_RESET=earliest make tail
```

### Performance Tuning

- **Increase event rate**: `EVENTS_PER_SEC=500 make gen`
- **Multiple consumers**: Run `make tail` in multiple terminals
- **Topic partitions**: Modify `create-topic` command for more partitions

## Development

### Adding New Fields

1. Update `data/schema.py` with new field definition
2. Add validation rules if needed
3. Update `data/generator.py` to generate the field
4. Add tests in `tests/test_schema.py`

### Extending the Pipeline

This Phase 1 implementation provides the foundation for:
- Real-time anomaly detection
- Multi-stage processing pipelines
- Data persistence and analytics
- Alerting and monitoring

## License

See [LICENSE](LICENSE) file for details.
