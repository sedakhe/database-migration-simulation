# Database Migration Simulation

A comprehensive simulation of a database migration scenario using Change Data Capture (CDC) events, Apache Kafka, Apache Flink, and PostgreSQL. This project demonstrates real-time data processing and enrichment in a microservices architecture.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source DB     │───▶│     Kafka    │───▶│   Flink Job     │───▶│  Target DB      │
│  (Simulated)    │    │   (CDC Topic)│    │  (Processing)   │    │  (PostgreSQL)   │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         │                       │                       │                       │
         ▼                       ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ CDC Producer    │    │ Kafka UI     │    │ Python UDFs     │    │ Enriched Data   │
│ (Python)        │    │ (Monitoring) │    │ (Email Domain)  │    │ (Analytics)     │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for running the CDC producer locally)
- Git

### 1. Clone and Setup

```bash
git clone <repository-url>
cd database-migration-simulation
```

### 2. Start the Infrastructure

```bash
cd docker
docker-compose up -d
```

This will start:
- **Zookeeper** (port 2181)
- **Kafka** (port 9092)
- **PostgreSQL** (port 5432)
- **Flink JobManager** (port 8081)
- **Flink TaskManager**
- **Kafka UI** (port 8080)

### 3. Wait for Services to Start

```bash
# Check if all services are healthy
docker-compose ps

# Wait for Flink to be ready (check logs)
docker-compose logs flink-jobmanager
```

### 4. Submit the Flink Job

```bash
# Submit the SQL job to Flink
docker exec -it flink-jobmanager ./bin/sql-client.sh embedded -f /opt/flink/usrlib/flink_job/job.sql
```

### 5. Run the CDC Producer

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run the CDC producer
python src/producers/cdc_producer.py --continuous
```

## 📊 Monitoring and Observability

### Web UIs

- **Flink Dashboard**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: `psql -h localhost -p 5432 -U postgres -d migration_db`

### Key Metrics to Monitor

1. **Kafka Topic**: `cdc-events` - Monitor message throughput
2. **Flink Job**: Check processing latency and checkpointing
3. **PostgreSQL**: Monitor insert rates and table growth

## 🗂️ Project Structure

```
database-migration-simulation/
├── src/
│   ├── flink_job/
│   │   ├── job.sql                    # Main Flink SQL job
│   │   └── pyudf_email_domain.py     # Python UDFs for data enrichment
│   ├── producers/
│   │   └── cdc_producer.py           # CDC event producer
│   └── schemas/
│       └── postgres.sql              # PostgreSQL schema definition
├── data/
│   └── sample_events.json            # Sample CDC events
├── docker/
│   ├── docker-compose.yml            # Infrastructure setup
│   └── postgres-init.sql             # Database initialization
├── README.md
└── requirements.txt
```

## 🔧 Configuration

### Environment Variables

The CDC producer supports the following configuration:

```bash
# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=cdc-events

# Database configuration
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=migration_db
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
```

### Flink Configuration

Key Flink settings in `docker-compose.yml`:

- **Checkpointing**: 10-second intervals with exactly-once semantics
- **Parallelism**: 2 for both job and table execution
- **Mini-batch**: Enabled for better throughput
- **State Backend**: Filesystem-based for persistence

## 📈 Data Processing Pipeline

### 1. CDC Event Structure

```json
{
  "id": 1,
  "first_name": "John",
  "last_name": "Doe",
  "email": "john.doe@example.com",
  "phone": "+1-555-0123",
  "address": "123 Main St",
  "city": "New York",
  "state": "NY",
  "country": "USA",
  "postal_code": "10001",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z",
  "source_system": "legacy_system",
  "cdc_operation": "INSERT",
  "cdc_timestamp": "2024-01-15T10:30:00Z",
  "cdc_sequence": 1234,
  "cdc_source": "legacy_database"
}
```

### 2. Data Enrichment

The Flink job performs the following enrichments:

- **Email Domain Extraction**: Using Python UDF
- **Email Validation**: Check format validity
- **Email Categorization**: Personal, corporate, educational, etc.
- **Country Inference**: From postal codes when missing
- **Full Name Construction**: Concatenate first and last names
- **Data Normalization**: Standardize email formats

### 3. Output Schema

```sql
CREATE TABLE users_enriched (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    email_domain VARCHAR(100),        -- Extracted domain
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_system VARCHAR(50),
    migration_timestamp TIMESTAMP,
    cdc_operation VARCHAR(10),
    full_name VARCHAR(200),           -- Constructed full name
    email_category VARCHAR(20),       -- Domain category
    inferred_country VARCHAR(50),     -- Inferred from postal code
    is_valid_email_flag BOOLEAN,      -- Email validation
    normalized_email VARCHAR(255)     -- Normalized email
);
```

## 🧪 Testing

### Run the CDC Producer

```bash
# Single batch run
python src/producers/cdc_producer.py

# Continuous simulation
python src/producers/cdc_producer.py --continuous --interval 30

# Custom configuration
python src/producers/cdc_producer.py \
    --bootstrap-servers localhost:9092 \
    --topic cdc-events \
    --data-file data/sample_events.json \
    --continuous
```

### Verify Data Processing

```sql
-- Connect to PostgreSQL
psql -h localhost -p 5432 -U postgres -d migration_db

-- Check processed records
SELECT COUNT(*) FROM users_enriched;

-- View enriched data
SELECT 
    id, 
    full_name, 
    email, 
    email_domain, 
    email_category,
    country,
    cdc_operation,
    migration_timestamp
FROM users_enriched 
ORDER BY migration_timestamp DESC 
LIMIT 10;

-- Check analytics view
SELECT * FROM user_analytics ORDER BY user_count DESC;
```

## 🔍 Troubleshooting

### Common Issues

1. **Flink Job Not Starting**
   ```bash
   # Check Flink logs
   docker-compose logs flink-jobmanager
   docker-compose logs flink-taskmanager
   ```

2. **Kafka Connection Issues**
   ```bash
   # Verify Kafka is running
   docker-compose ps kafka
   
   # Check Kafka logs
   docker-compose logs kafka
   ```

3. **PostgreSQL Connection Issues**
   ```bash
   # Test database connection
   docker exec -it postgres psql -U postgres -d migration_db -c "SELECT 1;"
   ```

4. **Python UDF Issues**
   ```bash
   # Test UDFs locally
   python src/flink_job/pyudf_email_domain.py
   ```

### Performance Tuning

1. **Increase Parallelism**
   - Modify `table.exec.resource.default-parallelism` in Flink
   - Scale TaskManager instances

2. **Optimize Checkpointing**
   - Adjust checkpoint interval based on latency requirements
   - Tune checkpoint timeout and min-pause

3. **Kafka Optimization**
   - Increase partition count for better parallelism
   - Tune producer batch size and linger time

## 📚 Additional Resources

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Flink SQL Reference](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🎯 Interview Notes

This project demonstrates:

- **Real-time Data Processing**: Using Apache Flink for stream processing
- **Event-Driven Architecture**: Kafka-based CDC simulation
- **Data Enrichment**: Python UDFs for complex transformations
- **Exactly-Once Processing**: Flink checkpointing and state management
- **Monitoring and Observability**: Multiple monitoring interfaces
- **Scalable Design**: Docker-based infrastructure with configurable parallelism
- **Data Quality**: Validation, normalization, and error handling
- **Analytics**: Real-time views and aggregations

The implementation showcases production-ready patterns for data migration and real-time processing scenarios commonly found in enterprise environments.