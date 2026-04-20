#!/bin/bash
# =============================================================
# scripts/setup_linux.sh
# TASK 1 — Linux + File System Task
# Sets up directory structure, permissions, automated file
# movement, and operation logging for the data pipeline.
# =============================================================

LOG_FILE="logs/pipeline_ops.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p logs data/raw data/processed data/warehouse data/archive

log_op() {
    echo "[$TIMESTAMP] [INFO] $1" | tee -a "$LOG_FILE"
}

log_op "=== Pipeline Environment Setup Started ==="

# ── 1. Create structured directories ──────────────────────────
DIRS=(
    "data/raw/csv"
    "data/raw/json"
    "data/processed/daily"
    "data/processed/monthly"
    "data/warehouse/fact"
    "data/warehouse/dim"
    "data/archive"
    "logs/ingestion"
    "logs/transform"
    "logs/errors"
    "tmp"
)

for dir in "${DIRS[@]}"; do
    mkdir -p "$dir"
    log_op "Created directory: $dir"
done

# ── 2. Set file permissions ────────────────────────────────────
# Owner: full access | Group: read+execute | Others: none
chmod 750 data/
chmod 750 logs/
chmod 700 tmp/
chmod -R 640 data/raw/ 2>/dev/null || true
log_op "File permissions set (750 on data/, 700 on tmp/)"

# ── 3. Create a .env template ─────────────────────────────────
cat > .env.template << 'EOF'
DB_HOST=localhost
DB_PORT=5432
DB_NAME=attendance_db
DB_USER=admin
DB_PASSWORD=changeme
KAFKA_BOOTSTRAP=localhost:9092
AWS_BUCKET=attendance-data-lake
GCP_PROJECT=attendance-project
AIRFLOW_HOME=./airflow
EOF
log_op "Created .env.template"

# ── 4. Automate file movement: move processed CSVs to archive ──
move_processed_files() {
    local count=0
    for f in data/processed/daily/*.csv; do
        [ -f "$f" ] || continue
        ARCHIVE_DIR="data/archive/$(date +%Y)/$(date +%m)"
        mkdir -p "$ARCHIVE_DIR"
        mv "$f" "$ARCHIVE_DIR/"
        log_op "Archived: $f → $ARCHIVE_DIR/"
        ((count++))
    done
    log_op "Archived $count files total."
}

# ── 5. Disk usage report ───────────────────────────────────────
log_op "Disk usage summary:"
du -sh data/* 2>/dev/null | while read line; do
    log_op "  $line"
done

# ── 6. Set up a cron-like log rotation stub ───────────────────
cat > scripts/rotate_logs.sh << 'ROTATE'
#!/bin/bash
# Log rotation: compress logs older than 7 days
find logs/ -name "*.log" -mtime +7 -exec gzip {} \;
find logs/ -name "*.log.gz" -mtime +30 -delete
echo "$(date) — Log rotation complete" >> logs/pipeline_ops.log
ROTATE
chmod +x scripts/rotate_logs.sh
log_op "Log rotation script created at scripts/rotate_logs.sh"

# ── 7. Validate environment ────────────────────────────────────
log_op "Checking Python version..."
python3 --version >> "$LOG_FILE" 2>&1

log_op "=== Setup Complete ==="
echo ""
echo "✅  Linux environment set up successfully!"
echo "    Logs written to: $LOG_FILE"
