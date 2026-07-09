# hazshield-ingest — Session 1 skeleton

## Build (on the control machine)
    cargo build --release
    # binary: target/release/hazshield-ingest

## Deploy to edge
    ssh hazshield-edge 'sudo mkdir -p /opt/hazshield-ingest/spool && sudo chown -R hazshield:hazshield /opt/hazshield-ingest'
    # Run the command as follows in your control machine
    bash deploy.sh
    # EDIT the two CHANGE_ME passwords in the unit first (or use a drop-in)

## Session 1 checkpoints
- [ ] local:  curl -s http://127.0.0.1:8020/healthz   (from edge VM)
- [ ] logs:   journalctl -u hazshield-ingest -f  -> JSON events
- [ ] public: curl -s https://ingest.<domain>/healthz  (Caddy already routes it!)
- [ ] shutdown: sudo systemctl stop hazshield-ingest -> logs show
      "SIGTERM received" then "drained and stopped" (not killed)
