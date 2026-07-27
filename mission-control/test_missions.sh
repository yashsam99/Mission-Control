#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8080"
PASS="\033[32mPASS\033[0m"
FAIL="\033[31mFAIL\033[0m"

echo "== 1. Health check =="
for i in $(seq 1 30); do
  if curl -sf "$BASE/health" >/dev/null; then
    echo -e "commander healthy: $PASS"
    break
  fi
  sleep 2
  if [ "$i" -eq 30 ]; then echo -e "commander health: $FAIL"; exit 1; fi
done

status_of() { curl -s "$BASE/missions/$1" | jq -r '.status' 2>/dev/null || echo "UNKNOWN"; }

echo "== 2. Single mission lifecycle =="
MID=$(curl -s -X POST "$BASE/missions" -H 'Content-Type: application/json' \
      -d '{"objective":"single recon"}' | jq -r '.mission_id')
echo "mission_id=$MID"
S=$(status_of "$MID")
[ "$S" = "QUEUED" ] || [ "$S" = "IN_PROGRESS" ] && echo -e "initial ($S): $PASS" || { echo -e "initial ($S): $FAIL"; exit 1; }
sleep 6
echo "after 6s: $(status_of "$MID")"
sleep 12
FINAL=$(status_of "$MID")
if [ "$FINAL" = "COMPLETED" ] || [ "$FINAL" = "FAILED" ]; then
  echo -e "terminal ($FINAL): $PASS"
else
  echo -e "terminal ($FINAL): $FAIL"; exit 1
fi

echo "== 3. Concurrency flood (20 missions) =="
IDS=()
for i in $(seq 1 20); do
  ID=$(curl -s -X POST "$BASE/missions" -H 'Content-Type: application/json' \
       -d "{\"objective\":\"flood-$i\"}" | jq -r '.mission_id')
  IDS+=("$ID")
done
sleep 7
INPROG=0
for ID in "${IDS[@]}"; do
  [ "$(status_of "$ID")" = "IN_PROGRESS" ] && INPROG=$((INPROG+1)) || true
done
echo "missions IN_PROGRESS concurrently: $INPROG"
[ "$INPROG" -ge 2 ] && echo -e "concurrency: $PASS" || { echo -e "concurrency: $FAIL"; exit 1; }

echo "== 4. Identity rotation audit (35s window) =="
SOLDIER=$(docker compose ps -q soldier | head -n1 || true)
if [ -z "$SOLDIER" ]; then echo -e "find soldier: $FAIL"; exit 1; fi
echo "watching $SOLDIER for 'Token Rotated' over 35s..."
sleep 35
ROT=$(docker logs "$SOLDIER" 2>&1 | grep -c "Token Rotated" || true)
echo "Token Rotated count: $ROT"
[ "$ROT" -ge 1 ] && echo -e "rotation: $PASS" || { echo -e "rotation: $FAIL"; exit 1; }

echo "== All scenarios passed =="
