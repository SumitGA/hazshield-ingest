# hazshield-ingest — Session 1 skeleton

## Build (on the control machine)
    cargo build --release
    # binary: target/release/hazshield-ingest

## Deploy to edge
    ssh hazshield-edge 'sudo mkdir -p /opt/hazshield-ingest/spool && sudo chown -R hazshield:hazshield /opt/hazshield-ingest'
    scp target/release/hazshield-ingest hazshield-edge:/tmp/
    ssh hazshield-edge 'sudo mv /tmp/hazshield-ingest /opt/hazshield-ingest/ && sudo chown hazshield:hazshield /opt/hazshield-ingest/hazshield-ingest'
    scp deploy/hazshield-ingest.service hazshield-edge:/tmp/
    ssh hazshield-edge 'sudo mv /tmp/hazshield-ingest.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now hazshield-ingest'
    # EDIT the two CHANGE_ME passwords in the unit first (or use a drop-in)

## Session 1 checkpoints
- [ ] local:  curl -s http://127.0.0.1:8020/healthz   (from edge VM)
- [ ] logs:   journalctl -u hazshield-ingest -f  -> JSON events
- [ ] public: curl -s https://ingest.<domain>/healthz  (Caddy already routes it!)
- [ ] shutdown: sudo systemctl stop hazshield-ingest -> logs show
      "SIGTERM received" then "drained and stopped" (not killed)
