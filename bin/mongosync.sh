#!/usr/bin/env bash
# Start sync.
# Prefer Maven exec from repo; fall back to dist fat jar if present.
#
# Usage:
#   ./bin/mongosync.sh
#   ./bin/mongosync.sh -f doc/examples/mongo-sync.example.properties
#   ./bin/mongosync.sh --config ./my.properties
#   CONF=./my.properties ./bin/mongosync.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAIN=com.whaleal.third.mongo.sync.launcher.SyncMain
ARGS=()
SHUTDOWN=false
CONFIG_PATH=""
PID_FILE=""
CHILD_PID=""
EXIT_USAGE=2
EXIT_ALREADY_RUNNING=20
EXIT_PID_NOT_FOUND=21
EXIT_STALE_PID=22

fail() {
  local code="$1"
  shift
  echo "[sync] ERROR code=$code message=$*" >&2
  exit "$code"
}

usage() {
  cat <<'EOF'
Usage:
  ./bin/mongosync.sh [-f <config.properties>] [sync args...]
  ./bin/mongosync.sh [config.properties]
  ./bin/mongosync.sh --config <config.properties> --shutdown

Options:
  -f, --config FILE   指定配置文件
  --shutdown          优雅关闭该配置对应的 mongosync 进程
  -h, --help          显示帮助

Examples:
  ./bin/mongosync.sh -f doc/examples/mongo-sync.example.properties
  ./bin/mongosync.sh --progress-log-seconds 5 -f my-sync.properties
  ./bin/mongosync.sh --config my-sync.properties --shutdown
EOF
}

resolve_config() {
  if [ -n "$CONFIG_PATH" ]; then
    printf '%s\n' "$CONFIG_PATH"
    return
  fi
  if [ ${#ARGS[@]} -gt 0 ] && [[ "${ARGS[0]}" != --* ]]; then
    printf '%s\n' "${ARGS[0]}"
    return
  fi
  if [ -f "${CONF:-}" ]; then
    printf '%s\n' "$CONF"
    return
  fi
  if [ -f "$ROOT/dist/conf/mongo-sync.properties" ]; then
    printf '%s\n' "$ROOT/dist/conf/mongo-sync.properties"
    return
  fi
  printf '%s\n' "$ROOT/doc/examples/mongo-sync.example.properties"
}

resolve_pid_file() {
  local config="$1"
  local abs
  local sum
  abs="$(cd "$(dirname "$config")" && pwd)/$(basename "$config")"
  sum="$(printf '%s' "$abs" | cksum | awk '{print $1}')"
  mkdir -p "$ROOT/run"
  printf '%s\n' "$ROOT/run/mongosync-${sum}.pid"
}

while [ $# -gt 0 ]; do
  case "$1" in
    -f|--config)
      if [ $# -lt 2 ]; then
        fail "$EXIT_USAGE" "missing value for $1"
      fi
      CONFIG_PATH="$2"
      ARGS+=("$2")
      shift 2
      ;;
    --shutdown)
      SHUTDOWN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

if [ ${#ARGS[@]} -eq 0 ]; then
  ARGS=("$(resolve_config)")
fi

CONFIG_PATH="$(resolve_config)"
PID_FILE="$(resolve_pid_file "$CONFIG_PATH")"

if [ "$SHUTDOWN" = true ]; then
  if [ ! -f "$PID_FILE" ]; then
    fail "$EXIT_PID_NOT_FOUND" "pid file not found: $PID_FILE"
  fi
  PID="$(cat "$PID_FILE")"
  if [ -z "$PID" ]; then
    rm -f "$PID_FILE"
    fail "$EXIT_STALE_PID" "pid file is empty and has been removed: $PID_FILE"
  fi
  if ! kill -0 "$PID" >/dev/null 2>&1; then
    echo "[sync] process not running, removing stale pid file: $PID_FILE" >&2
    rm -f "$PID_FILE"
    exit "$EXIT_STALE_PID"
  fi
  echo "[sync] shutting down pid=$PID config=$CONFIG_PATH" >&2
  kill -TERM "$PID"
  exit 0
fi

JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx2g}"
FAT="$ROOT/dist/lib/mongo-sync-all.jar"

echo "[sync] config=${ARGS[*]}" >&2
if [ -f "$PID_FILE" ]; then
  EXISTING_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" >/dev/null 2>&1; then
    fail "$EXIT_ALREADY_RUNNING" "already running pid=$EXISTING_PID config=$CONFIG_PATH"
  fi
  rm -f "$PID_FILE"
fi
cleanup_pid() {
  if [ -n "${PID_FILE:-}" ] && [ -f "$PID_FILE" ]; then
    CURRENT_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "${CHILD_PID:-}" ] && [ "$CURRENT_PID" = "$CHILD_PID" ]; then
      rm -f "$PID_FILE"
    fi
  fi
}

forward_term() {
  if [ -n "${CHILD_PID:-}" ] && kill -0 "$CHILD_PID" >/dev/null 2>&1; then
    kill -TERM "$CHILD_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup_pid EXIT
trap forward_term INT TERM

launch_child() {
  if [ -f "$FAT" ]; then
    # shellcheck disable=SC2086
    java $JAVA_OPTS -cp "$FAT" "$MAIN" "${ARGS[@]}" &
  else
    (
      cd "$ROOT"
      exec mvn -q -pl mongo-sync-client -am package -DskipTests exec:java \
        -Dexec.mainClass="$MAIN" \
        -Dexec.classpathScope=runtime \
        -Dexec.args="${ARGS[*]}"
    ) &
  fi
  CHILD_PID="$!"
  echo "$CHILD_PID" > "$PID_FILE"
}

launch_child
wait "$CHILD_PID"
