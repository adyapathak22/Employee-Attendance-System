#!/bin/bash
# Log rotation: compress logs older than 7 days
find logs/ -name "*.log" -mtime +7 -exec gzip {} \;
find logs/ -name "*.log.gz" -mtime +30 -delete
echo "$(date) — Log rotation complete" >> logs/pipeline_ops.log
