#!/usr/bin/env bash
# submit a job to the vidura queue.
# Usage: jobsubmit NAME [--attempts N] [--timeout SECS] -- COMMAND...
set -eu
name="$1"; shift
att=3; tmo=0
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
  case "$1" in
    --attempts) att="$2"; shift 2;;
    --timeout) tmo="$2"; shift 2;;
    *) echo "unknown opt $1" >&2; exit 2;;
  esac
done
[ "${1:-}" = "--" ] && shift
[ "$#" -gt 0 ] || { echo "no command" >&2; exit 2; }

BASE="${JOBS_BASE:-$HOME/jobs}"
mkdir -p "$BASE/jobs"
f="$BASE/jobs/$name.job"
{
  echo "#MAX_ATTEMPTS: $att"
  echo "#TIMEOUT_SECS: $tmo"
  echo "#SUBMITTED: $(date '+%F %T')"
  echo "set -eu"
  echo "$*"
} > "$f"
echo "queued: $f"
