#!/usr/bin/env bash
set -euo pipefail
HOST=hazshield-edge        # your ~/.ssh/config alias
DEST=/opt/hazshield-ingest

SHA=$(git rev-parse --short HEAD)
[ -n "$(git status --porcelain)" ] && echo "WARN: deploying dirty tree (${SHA}-dirty)"

cargo build --release
scp -q target/release/hazshield-ingest "$HOST:/tmp/hazshield-ingest.new"
ssh "$HOST" "sudo install -m755 -o hazshield -g hazshield /tmp/hazshield-ingest.new $DEST/hazshield-ingest \
             && sudo systemctl restart hazshield-ingest && rm /tmp/hazshield-ingest.new"

sleep 2
DEPLOYED=$(curl -sf https://ingest.sumitgautam.tech/version | grep -o '"git_sha":"[^"]*"' | cut -d'"' -f4)
if [ "$DEPLOYED" = "$SHA" ] || [ "$DEPLOYED" = "${SHA}-dirty" ]; then
    echo "✓ deploy verified: $DEPLOYED live"
else
    echo "✗ MISMATCH: built $SHA but edge is running ${DEPLOYED:-nothing}" >&2
    exit 1
fi