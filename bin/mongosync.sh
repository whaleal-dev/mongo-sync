#!/usr/bin/env bash
# Start sync.
# Prefer Maven exec from repo; fall back to dist fat jar if present.
#
# Usage:
#   ./bin/mongosync.sh
#   ./bin/mongosync.sh doc/examples/mongo-sync.example.properties
#   CONF=./my.properties ./bin/mongosync.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAIN=com.whaleal.third.mongo.sync.launcher.SyncMain
ARGS=("$@")
if [ ${#ARGS[@]} -eq 0 ]; then
  if [ -f "${CONF:-}" ]; then
    ARGS=("$CONF")
  elif [ -f "$ROOT/dist/conf/mongo-sync.properties" ]; then
    ARGS=("$ROOT/dist/conf/mongo-sync.properties")
  else
    ARGS=("$ROOT/doc/examples/mongo-sync.example.properties")
  fi
fi

JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx2g}"
FAT="$ROOT/dist/lib/mongo-sync-all.jar"

echo "[sync] config=${ARGS[*]}" >&2

if [ -f "$FAT" ]; then
  # shellcheck disable=SC2086
  exec java $JAVA_OPTS -cp "$FAT" "$MAIN" "${ARGS[@]}"
fi

cd "$ROOT"
exec mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
  -Dexec.mainClass="$MAIN" \
  -Dexec.classpathScope=runtime \
  -Dexec.args="${ARGS[*]}"
