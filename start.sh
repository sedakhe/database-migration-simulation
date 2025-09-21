#!/bin/bash

# Database Migration Simulation - Startup Script
# This script helps you get the entire system up and running quickly

set -e

echo "🚀 Starting Database Migration Simulation..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Navigate to docker directory
cd docker

echo "📦 Starting infrastructure services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 30

echo "🔍 Checking service health..."

# Check if services are running
services=("zookeeper" "kafka" "postgres" "flink-jobmanager" "flink-taskmanager")
for service in "${services[@]}"; do
    if docker-compose ps | grep -q "$service.*Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service is not running"
    fi
done

echo ""
echo "🎉 Infrastructure is ready!"
echo ""
echo "📊 Access Points:"
echo "  - Flink Dashboard: http://localhost:8081"
echo "  - Kafka UI: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432 (user: postgres, password: postgres, db: migration_db)"
echo ""
echo "🔧 Next Steps:"
echo "  1. Submit the Flink job:"
echo "     docker exec -it flink-jobmanager ./bin/sql-client.sh embedded -f /opt/flink/usrlib/flink_job/job.sql"
echo ""
echo "  2. Run the CDC producer:"
echo "     cd .."
echo "     pip install -r requirements.txt"
echo "     python src/producers/cdc_producer.py --continuous"
echo ""
echo "  3. Monitor the processing in the web UIs above"
echo ""
echo "🛑 To stop all services:"
echo "     cd docker && docker-compose down"