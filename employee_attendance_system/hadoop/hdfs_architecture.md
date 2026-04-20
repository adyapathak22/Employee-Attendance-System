# Task 13 — HDFS Architecture

## Overview

HDFS (Hadoop Distributed File System) is designed to store very large files reliably
across commodity hardware. It follows a master/slave architecture.

```
                    ╔═══════════════════════╗
                    ║      NAMENODE          ║  ← Master (metadata only)
                    ║  - File namespace      ║
                    ║  - Block→DataNode map  ║
                    ║  - Edit log / FSImage  ║
                    ╚═══════════╤═══════════╝
                                │  Heartbeat + Block Reports
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
    ╔═════════════╗   ╔═════════════╗   ╔═════════════╗
    ║ DATANODE 1  ║   ║ DATANODE 2  ║   ║ DATANODE 3  ║
    ║  Block A    ║   ║  Block A    ║   ║  Block B    ║
    ║  Block C    ║   ║  Block B    ║   ║  Block C    ║
    ╚═════════════╝   ╚═════════════╝   ╚═════════════╝
        (Rack 1)          (Rack 1)          (Rack 2)
```

## Key Components

### NameNode (Master)
- Maintains the file system **namespace** (directory tree, file metadata)
- Stores the **block-to-DataNode mapping** in memory
- Persists metadata via **FSImage** (snapshot) + **EditLog** (changes)
- Does NOT store actual data — only metadata
- **Single point of failure** → use Secondary NameNode or HA NameNode in production

### DataNode (Slave)
- Stores actual **data blocks** on local disk
- Default block size: **128 MB** (configurable)
- Sends **heartbeats** to NameNode every 3 seconds
- Sends **block reports** periodically
- Replication factor: default **3** copies

### Secondary NameNode
- NOT a backup NameNode (despite the name)
- Periodically merges FSImage + EditLog to prevent EditLog from growing too large
- Creates a checkpoint

## Read Flow (Attendance CSV from HDFS)

```
Client → NameNode: "Where are blocks for /attendance/raw/attendance.csv?"
NameNode → Client: "Block 1 on DN1,DN2 | Block 2 on DN2,DN3 | ..."
Client → DataNode1: Read Block 1 directly
Client → DataNode2: Read Block 2 directly
(Client reads from nearest replica for performance)
```

## Write Flow (Uploading new attendance data)

```
Client → NameNode: "Create /attendance/raw/attendance_2025.csv"
NameNode → Client: "Write Block 1 to DN1 (pipeline: DN1→DN2→DN3)"
Client → DN1 → DN2 → DN3: Write block via pipeline replication
DN1,2,3 → NameNode: "Block 1 written successfully"
Client → NameNode: "File write complete"
```

## Simulated Storage — Attendance System

| Path | Content | Block Size | Replication |
|------|---------|-----------|-------------|
| /attendance/raw/employees.csv | 200 employees | 128MB | 3 |
| /attendance/raw/attendance.csv | ~50k records | 128MB | 3 |
| /attendance/processed/ | Cleaned parquet | 128MB | 2 |
| /attendance/warehouse/ | Star schema | 128MB | 2 |

## HDFS Commands for This Project

```bash
# Check cluster health
hdfs dfsadmin -report

# Upload attendance data
hdfs dfs -put data/raw/attendance.csv /attendance/raw/

# Check file blocks
hdfs fsck /attendance/raw/attendance.csv -files -blocks -locations

# Change replication factor
hdfs dfs -setrep -w 2 /attendance/processed/

# Check space
hdfs dfs -du -h /attendance/

# Enable trash (instead of permanent delete)
hdfs dfs -rm -skipTrash /attendance/tmp/old_data.csv
```

## OLTP vs OLAP vs HDFS

| Feature | OLTP (SQLite/Postgres) | OLAP (Warehouse) | HDFS |
|---------|----------------------|------------------|------|
| Data Size | GB | TB | PB |
| Query Type | Point lookups | Aggregations | Batch scans |
| Schema | Strict (DDL) | Star schema | Schema-on-read |
| Latency | Milliseconds | Seconds | Minutes |
| Updates | Yes | Limited | No (append-only) |
| Use Case | Live attendance app | Monthly reports | Historical archives |
