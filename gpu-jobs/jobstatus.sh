#!/usr/bin/env bash
# status/monitor for the vidura job queue. Usage: jobstatus [n_log_lines]
set -u
BASE="${JOBS_BASE:-$HOME/jobs}"
n="${1:-3}"
printf '%s\n' "=== vidura job queue  $(date '+%F %T') ==="
q=$(ls -1tr "$BASE"/jobs/*.job 2>/dev/null | wc -l)
printf 'queued: %s   ' "$q"
r=$(ls -1 "$BASE"/running/*.job 2>/dev/null | wc -l)
printf 'running: %s   ' "$r"
printf 'holding: %s   ' "$(ls -1 "$BASE"/holding/*.job 2>/dev/null | wc -l)"
printf 'done: %s   failed: %s\n' \
  "$(ls -1 "$BASE"/done/*.job 2>/dev/null | wc -l)" \
  "$(ls -1 "$BASE"/failed/*.job 2>/dev/null | wc -l)"

if [ "$r" -gt 0 ]; then
  for j in "$BASE"/running/*.job; do
    name=$(basename "$j" .job)
    att=$(cat "$BASE/state/$name.attempts" 2>/dev/null || echo '?')
    st=$(cat "$BASE/state/$name.started" 2>/dev/null)
    el="?"
    [ -n "${st:-}" ] && el="$(( ($(date +%s) - st) / 60 ))m"
    echo "-- RUNNING $name (attempt $att, elapsed $el)"
    tail -n "$n" "$BASE/logs/$name.a$att.log" 2>/dev/null
  done
fi

echo "-- QUEUE (next first)"
ls -1tr "$BASE"/jobs/*.job 2>/dev/null | head -10 | while read -r j; do
  echo "   $(basename "$j")  (max_att=$(grep -m1 '^#MAX_ATTEMPTS:' "$j" | sed 's/.*://'))"
done

if [ -s "$BASE/logs/failed.log" ]; then
  echo "-- RECENT FAILURES"
  tail -5 "$BASE/logs/failed.log"
fi
if [ -s "$BASE/logs/done.log" ]; then
  echo "-- RECENT DONE"
  tail -5 "$BASE/logs/done.log"
fi
echo "-- runner log tail"
tail -n "$n" "$BASE/runner.log" 2>/dev/null
