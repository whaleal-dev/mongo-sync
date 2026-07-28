#!/usr/bin/env bash
# Start verify.
# Prefer Maven exec from repo; fall back to dist fat jar if present.
#
# Usage:
#   ./bin/verify.sh
#   ./bin/verify.sh doc/examples/mongo-verify.example.properties
#   ./bin/verify.sh --source-uri ... --target-uri ... --source-db demo --source-coll orders
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAIN=com.whaleal.third.mongo.sync.verify.VerifyMain
ARGS=("$@")
if [ ${#ARGS[@]} -eq 0 ]; then
  if [ -f "${CONF:-}" ]; then
    ARGS=("$CONF")
  elif [ -f "$ROOT/dist/conf/mongo-verify.properties" ]; then
    ARGS=("$ROOT/dist/conf/mongo-verify.properties")
  else
    ARGS=("$ROOT/doc/examples/mongo-verify.example.properties")
  fi
fi

JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx1g}"
FAT="$ROOT/dist/lib/mongo-sync-all.jar"

echo "[verify] args=${ARGS[*]}" >&2

if [ -f "$FAT" ]; then
  # shellcheck disable=SC2086
  exec java $JAVA_OPTS -cp "$FAT" "$MAIN" "${ARGS[@]}"
fi

cd "$ROOT"
exec mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
  -Dexec.mainClass="$MAIN" \
  -Dexec.classpathScope=runtime \
  -Dexec.args="${ARGS[*]}"
