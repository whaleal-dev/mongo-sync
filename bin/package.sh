#!/usr/bin/env bash
# Assemble release bundle with shaded fat jar + scripts + conf.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DIST="$ROOT/dist"
VERSION="${VERSION:-$(awk -F'[<>]' '/<version>/{print $3; exit}' "$ROOT/pom.xml")}"
RELEASE_DIR="$DIST/mongo-sync-$VERSION"
ARCHIVE="$DIST/mongo-sync-$VERSION.tar.gz"

rm -rf "$DIST"
mkdir -p "$DIST/lib" "$DIST/conf" "$DIST/bin" "$DIST/data"

echo "[package] mvn package (shade) ..."
mvn -q -pl mongo-sync-client -am package -DskipTests

SHADE=$(ls "$ROOT/mongo-sync-client/target/mongo-sync-client-"*-all.jar 2>/dev/null | head -1 || true)
if [ -z "$SHADE" ] || [ ! -f "$SHADE" ]; then
  echo "[package] WARN: shaded *-all.jar missing; conf/bin still copied. Use ./bin/mongosync.sh via mvn." >&2
else
  cp -f "$SHADE" "$DIST/lib/mongo-sync-all.jar"
  echo "[package] fat jar -> $DIST/lib/mongo-sync-all.jar"
fi

cp -f "$ROOT/doc/examples/mongo-sync.example.properties" "$DIST/conf/mongo-sync.properties"
cp -f "$ROOT/doc/examples/mongo-verify.example.properties" "$DIST/conf/mongo-verify.properties"
cp -f "$ROOT/bin/mongosync.sh" "$DIST/bin/mongosync.sh"
cp -f "$ROOT/bin/verify.sh" "$DIST/bin/verify.sh"
cp -f "$ROOT/bin/README.md" "$DIST/bin/README.md"
chmod +x "$ROOT/bin/"*.sh "$DIST/bin/"*.sh
rm -f "$DIST/bin/sync.sh" 2>/dev/null || true

echo "[package] conf -> $DIST/conf"
ls "$DIST/conf"

mkdir -p "$RELEASE_DIR"
cp -R "$DIST/bin" "$RELEASE_DIR/bin"
cp -R "$DIST/conf" "$RELEASE_DIR/conf"
cp -R "$DIST/data" "$RELEASE_DIR/data"
mkdir -p "$RELEASE_DIR/lib"
if [ -f "$DIST/lib/mongo-sync-all.jar" ]; then
  cp -f "$DIST/lib/mongo-sync-all.jar" "$RELEASE_DIR/lib/mongo-sync-all.jar"
fi
cp -f "$ROOT/README.md" "$RELEASE_DIR/README.md"

tar -czf "$ARCHIVE" -C "$DIST" "mongo-sync-$VERSION"
echo "[package] release dir -> $RELEASE_DIR"
echo "[package] release tar -> $ARCHIVE"
