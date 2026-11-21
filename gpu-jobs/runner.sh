#!/usr/bin/env bash
# vidura job runner: sequential queue, per-job retries with exponential backoff,
# per-attempt logs, timeout support. Run under tmux session "jobrunner".
set -u
BASE="${JOBS_BASE:-$HOME/jobs}"
JOBS="$BASE/jobs" RUNNING="$BASE/running" DONE="$BASE/done" FAILED="$BASE/failed"
LOGS="$BASE/logs" HOLDING="$BASE/holding" STATE="$BASE/state" TMPD="$BASE/tmp"
mkdir -p "$JOBS" "$RUNNING" "$DONE" "$FAILED" "$LOGS" "$HOLDING" "$STATE" "$TMPD"
POLL="${POLL_SECS:-20}"

log() { echo "[$(date '+%F %T')] $*" >> "$BASE/runner.log"; }

# backoff by attempt number (secs): 2m, 5m, 15m, then 30m flat
backoff() { case $1 in 1) echo 120;; 2) echo 300;; 3) echo 900;; *) echo 1800;; esac; }

field() { # $1=job file  $2=key  $3=default
  local v
  v=$(grep -m1 "^#$2:" "$1" 2>/dev/null | sed "s/^#$2:[[:space:]]*//" || true)
  echo "${v:-$3}"
}

log "runner started (pid $$)"

while true; do
  # 1) move due holding jobs back to the queue
  for h in "$HOLDING"/*.job; do
    [ -e "$h" ] || break
    name=$(basename "$h" .job)
    due=$(cat "$STATE/$name.due" 2>/dev/null || echo 0)
    if [ "$(date +%s)" -ge "$due" ]; then
      mv "$h" "$JOBS/" && rm -f "$STATE/$name.due"
      log "requeued $name"
    fi
  done

  # 2) pick the oldest queued job
  job=$(ls -1tr "$JOBS"/*.job 2>/dev/null | head -1)
  if [ -z "${job:-}" ]; then sleep "$POLL"; continue; fi
  name=$(basename "$job" .job)
  mv "$job" "$RUNNING/"; job="$RUNNING/$name.job"

  max_att=$(field "$job" MAX_ATTEMPTS 3)
  timeout_s=$(field "$job" TIMEOUT_SECS 0)
  attempts=$(( $(cat "$STATE/$name.attempts" 2>/dev/null || echo 0) + 1 ))
  echo "$attempts" > "$STATE/$name.attempts"
  echo "$(date +%s)" > "$STATE/$name.started"

  log "START $name attempt $attempts/$max_att"
  echo "[$(date '+%F %T')] attempt $attempts/$max_att (timeout=${timeout_s}s)" \
      > "$LOGS/$name.a$attempts.log"
  if [ "$timeout_s" -gt 0 ]; then
      timeout --kill-after=30 "$timeout_s" bash "$job" >> "$LOGS/$name.a$attempts.log" 2>&1
  else
      bash "$job" >> "$LOGS/$name.a$attempts.log" 2>&1
  fi
  rc=$?
  dur=$(( $(date +%s) - $(cat "$STATE/$name.started") ))
  rm -f "$STATE/$name.started"

  if [ "$rc" -eq 0 ]; then
    mv "$RUNNING/$name.job" "$DONE/"
    echo "$name attempt=$attempts duration=${dur}s rc=0 $(date '+%F %T')" >> "$LOGS/done.log"
    rm -f "$STATE/$name.attempts"
    log "DONE  $name attempt=$attempts duration=${dur}s"
  elif [ "$attempts" -lt "$max_att" ]; then
    mv "$RUNNING/$name.job" "$HOLDING/"
    due=$(( $(date +%s) + $(backoff "$attempts") ))
    echo "$due" > "$STATE/$name.due"
    log "RETRY $name rc=$rc attempt=$attempts -> requeue at $(date -d "@$due" '+%T')"
  else
    mv "$RUNNING/$name.job" "$FAILED/"
    { echo "job: $name"; echo "attempts: $attempts"; echo "last rc: $rc";
      echo "duration: ${dur}s"; echo "--- last 30 log lines (a$attempts) ---";
      tail -30 "$LOGS/$name.a$attempts.log"; } > "$FAILED/$name.error"
    echo "$name attempts=$attempts rc=$rc $(date '+%F %T')" >> "$LOGS/failed.log"
    rm -f "$STATE/$name.attempts"
    log "FAIL  $name after $attempts attempts (rc=$rc)"
  fi
done
