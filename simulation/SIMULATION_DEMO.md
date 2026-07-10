# The Hazshield-Ingest Service checkpoint run — protocol

Target: 10,000 readings/sec sustained 10 min, Postgres killed mid-run,
p99 < 10ms, ZERO alarms lost.

## Setup (edge VM)
    pip install aiohttp --break-system-packages
    X=$(redis-cli -h 10.10.0.12 -a "$RPW" XLEN hazshield:violations)
    C=$(psql "$PG" -tAc "SELECT count(*) FROM sensor_readings")

## Run (edge VM, terminal 1)
    ./sim.py --pg "$PG" --rate 10000 --duration 600 --scenario plume,flood

## Chaos (terminal 2, at ~t=200s)
    ssh ubuntu@10.10.0.12 'sudo systemctl stop postgresql'
    sleep 45
    ssh ubuntu@10.10.0.12 'sudo systemctl start postgresql'
    # watch journalctl -u hazshield-ingest -f meanwhile:
    #   warm flush failed / backoff climbing / recovery flush
    # sim output: req_errors stays 0, degraded_batches may rise

## The audit (after FINAL prints)
Let V = warn+crit from the sim's final line.
    redis-cli -h 10.10.0.12 -a "$RPW" XLEN hazshield:violations
      -> must equal X + V            (every alarm accounted for)
    curl -s localhost:8020/metrics | grep hot_violations_lost_total
      -> must be 0                   (the sacred number)
    sim final line: client p99 < 10ms, req_errors = 0
    psql count vs C: grows by ~accepted minus rate_limited minus
      (readings shed/degraded during the outage window) — bulk may
      legitimately be less than sent; that's the design.

Pass = all four lines hold. Record the numbers: they ARE Part 2's blog.
