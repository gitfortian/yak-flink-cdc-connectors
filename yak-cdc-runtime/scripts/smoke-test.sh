#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
./scripts/check-connector-isolation.sh
FLINK_SHA512=${FLINK_SHA512:-$(curl -fsSL https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz.sha512 | awk '{print $1}')}
FLINK_CDC_SHA512=${FLINK_CDC_SHA512:-$(curl -fsSL https://archive.apache.org/dist/flink/flink-cdc-3.6.0/flink-cdc-3.6.0-bin.tar.gz.sha512 | awk '{print $1}')}
test ${#FLINK_SHA512} -eq 128
test ${#FLINK_CDC_SHA512} -eq 128
image=yak-cdc-runtime:0.1.0-phase0
docker build -f yak-cdc-runtime/Dockerfile --build-arg "FLINK_SHA512=$FLINK_SHA512" --build-arg "FLINK_CDC_SHA512=$FLINK_CDC_SHA512" -t "$image" .
cid=$(docker run -d -p 127.0.0.1::8080 "$image")
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
port=$(docker port "$cid" 8080/tcp | sed 's/.*://')
for _ in $(seq 1 30); do curl -fsS "http://127.0.0.1:$port/health" && break; sleep 1; done
curl -fsS "http://127.0.0.1:$port/capabilities" | grep -q '"deliverySemantics": "at-least-once"'
safe=$'source:\n  type: mysql\n  password: ${ENV:SOURCE_PASSWORD}\nsink:\n  type: yak-jdbc\n  password: ${ENV:SINK_PASSWORD}'
curl -fsS -X POST --data-binary "$safe" "http://127.0.0.1:$port/validate" | grep -q '"valid":true'
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST --data-binary $'source:\n type: mysql\n password: secret\nsink:\n type: yak-jdbc' "http://127.0.0.1:$port/validate")
test "$code" = 422
curl -fsS "http://127.0.0.1:$port/status" | grep -q '"status":"NONE"'
test "$(docker inspect -f '{{.Config.User}}' "$cid")" = 10001
