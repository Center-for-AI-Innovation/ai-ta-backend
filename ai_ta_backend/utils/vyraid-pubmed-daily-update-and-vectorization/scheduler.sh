#!/bin/bash
# PubMed Pipeline Scheduler
# Replaces cron when crontab is unavailable (e.g. PAM-restricted HPC systems).
#
# Runs as a persistent background process:
#   - Stage 1 (download): daily at 2:00 AM
#   - Stage 2 (vectorize): every 30 minutes
#
# Start:   nohup ./scheduler.sh >> /tmp/pubmed_scheduler.log 2>&1 &
# Stop:    kill $(cat /tmp/pubmed_scheduler.pid)
# Status:  tail -f /tmp/pubmed_scheduler.log

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="/tmp/pubmed_scheduler.pid"
LOG_PREFIX="[scheduler]"

# Prevent duplicate scheduler instances
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Already running (PID $OLD_PID). Exiting."
        exit 0
    fi
fi
echo $$ > "$PIDFILE"

echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Started (PID $$)"
echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Stage 1 runs daily at 02:00 | Stage 2 runs every 30 min"

# Track last run times (seconds since epoch)
last_stage1_day=""         # Track which calendar day Stage 1 last ran
last_stage2_run=0          # Epoch seconds

STAGE2_INTERVAL=1800       # 30 minutes in seconds
STAGE1_HOUR=2              # 2:00 AM

run_stage1() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Triggering Stage 1 (daily download)..."
    bash "$SCRIPT_DIR/run_daily_updates.sh" >> /tmp/pubmed_stage1.log 2>&1
    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Stage 1 finished"
}

run_stage2() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Triggering Stage 2 (vectorization)..."
    bash "$SCRIPT_DIR/run_vectorization.sh" >> /tmp/pubmed_stage2.log 2>&1
    echo "$(date '+%Y-%m-%d %H:%M:%S') $LOG_PREFIX Stage 2 finished"
    last_stage2_run=$(date +%s)
}

while true; do
    now=$(date +%s)
    current_hour=$(date +%-H)
    current_day=$(date +%Y-%m-%d)

    # Stage 1: once per day at STAGE1_HOUR
    if [ "$current_hour" -eq "$STAGE1_HOUR" ] && [ "$last_stage1_day" != "$current_day" ]; then
        last_stage1_day="$current_day"
        run_stage1 &
    fi

    # Stage 2: every STAGE2_INTERVAL seconds (runs in background, flock-guarded)
    elapsed=$(( now - last_stage2_run ))
    if [ "$elapsed" -ge "$STAGE2_INTERVAL" ]; then
        last_stage2_run=$now   # set optimistically to prevent pile-up
        run_stage2 &
    fi

    sleep 60   # Check every minute
done
