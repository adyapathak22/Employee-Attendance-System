#!/bin/bash
# hadoop/hdfs_setup.sh
# TASK 12 — Hadoop: Set up HDFS locally and upload structured/unstructured data
# NOTE: Requires Docker or a local Hadoop installation.

echo "======================================================"
echo "  TASK 12: HDFS Setup for Employee Attendance System"
echo "======================================================"

# Option A: Use Docker (recommended for local dev)
start_hdfs_docker() {
    echo "[INFO] Starting HDFS via Docker Compose..."
    cat > /tmp/hdfs-docker-compose.yml << 'DOCKEREOF'
version: '3'
services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
      - "9000:9000"
    environment:
      - CLUSTER_NAME=attendance-cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
      - ./data:/data

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    depends_on:
      - namenode
    ports:
      - "9864:9864"
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - hadoop_datanode:/hadoop/dfs/data

volumes:
  hadoop_namenode:
  hadoop_datanode:
DOCKEREOF
    docker-compose -f /tmp/hdfs-docker-compose.yml up -d
    echo "[INFO] Waiting for HDFS to start..."
    sleep 20
}

# HDFS directory structure for attendance system
create_hdfs_dirs() {
    echo "[INFO] Creating HDFS directory structure..."
    HDFS_CMD="docker exec namenode hdfs dfs"

    $HDFS_CMD -mkdir -p /attendance/raw/csv
    $HDFS_CMD -mkdir -p /attendance/raw/json
    $HDFS_CMD -mkdir -p /attendance/processed/daily
    $HDFS_CMD -mkdir -p /attendance/processed/monthly
    $HDFS_CMD -mkdir -p /attendance/warehouse
    $HDFS_CMD -mkdir -p /attendance/archive
    $HDFS_CMD -mkdir -p /attendance/logs
    $HDFS_CMD -chmod -R 755 /attendance
    echo "[INFO] HDFS directories created."
    $HDFS_CMD -ls /attendance/
}

# Upload CSV files to HDFS
upload_data() {
    echo "[INFO] Uploading CSV data to HDFS..."
    for f in data/raw/*.csv; do
        fname=$(basename "$f")
        docker exec -i namenode hdfs dfs -put - /attendance/raw/csv/$fname < $f && \
            echo "[INFO] Uploaded: $fname → hdfs:///attendance/raw/csv/$fname" || \
            echo "[WARN] Failed to upload $fname"
    done
}

# Verify data
verify_hdfs() {
    echo "[INFO] HDFS report:"
    docker exec namenode hdfs dfs -du -h /attendance/
    docker exec namenode hdfs dfsadmin -report | head -30
}

echo "[INFO] To use HDFS via Docker, run these steps:"
echo "  1. docker-compose -f /tmp/hdfs-docker-compose.yml up -d"
echo "  2. Wait ~20 seconds for HDFS to initialize"
echo "  3. Run: docker exec namenode hdfs dfs -mkdir -p /attendance/raw"
echo "  4. Run: docker exec namenode hdfs dfs -put data/raw/*.csv /attendance/raw/"
echo "  5. Verify: docker exec namenode hdfs dfs -ls /attendance/raw/"
echo "  6. Web UI: http://localhost:9870"
echo ""
echo "[INFO] Manual HDFS commands reference:"
echo "  hdfs dfs -ls /attendance/raw/         → List files"
echo "  hdfs dfs -du -h /attendance/           → Disk usage"
echo "  hdfs dfs -cat /attendance/raw/employees.csv | head"
echo "  hdfs dfs -cp /attendance/raw/ /attendance/archive/"
echo "  hdfs dfs -rm -r /attendance/tmp/"
echo "  hdfs dfsadmin -report                  → Cluster report"
