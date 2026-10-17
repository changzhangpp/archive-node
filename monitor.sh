#!/bin/bash
# archive-node availability monitor: dual probe every 30s, log state changes + hourly heartbeat
cd "$(dirname "$0")" || exit 1
EXT_URL="https://8100-572cc49902f2ee46.monkeycode-ai.live/health"
LOG=monitor.log
prev=""
n=0
echo "[$(date '+%F %T')] monitor start (L=local E=external)" >> "$LOG"
while true; do
  ts=$(date '+%F %T')
  l=$(curl -s -m5 -o /dev/null -w '%{http_code}' http://127.0.0.1:8100/health 2>/dev/null)
  e=$(curl -s -m10 -o /dev/null -w '%{http_code}' "$EXT_URL" 2>/dev/null)
  [ -z "$l" ] && l=000
  [ -z "$e" ] && e=000
  cur="L$l E$e"
  if [ "$cur" != "$prev" ]; then
    echo "[$ts] STATE ${prev:-init} -> $cur" >> "$LOG"
    prev="$cur"
  fi
  n=$((n+1))
  if [ $((n % 120)) -eq 0 ]; then
    echo "[$ts] HB $cur" >> "$LOG"
  fi
  sleep 30
done
