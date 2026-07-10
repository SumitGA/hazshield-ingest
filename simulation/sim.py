#!/usr/bin/env python3
"""HazShield plant simulator.

Simulates thousands of sensors with mean-reverting physics, scripted
hazard scenarios, and deliberate abuse, at a controllable aggregate rate.

    ./sim.py --rate 10000 --duration 60 --scenario plume,flood

Design notes (the parts that make load tests honest):

  * OU PROCESS per sensor: x += theta*(mu-x)*dt + sigma*sqrt(dt)*N(0,1).
    Mean-reverting noise looks like real telemetry: wanders, returns.
    Baseline mu sits at 40% of warn for high-alarm sensors (110% of warn
    for low-alarm kinds) so steady state produces ZERO violations —
    every alarm in the run is CAUSED by a scenario, so alarm counts are
    predictable and loss is detectable.

  * SHARDED WORKERS, not per-sensor tasks: N workers each own a slice of
    the sensor population and emit batched readings on a tick. 3000
    sensors cost 3000 floats of state, not 3000 tasks.

  * CLIENT-SIDE LATENCY: every request is timed here, p50/p99 reported.
    The gateway's own histogram measures handler time; this measures
    what a CALLER experiences (network + serde + handler). Report both,
    trust the worse one.

  * SCENARIOS mutate mu, not x: the plume RAISES THE MEAN of gas sensors
    in one zone and physics drags readings up over seconds — a realistic
    onset ramp, not a step function. The flood ignores physics entirely
    and hammers one 1Hz sensor at ~100 msg/s to provoke the limiter.
"""
import argparse, asyncio, csv, json, math, random, statistics, subprocess, sys, time

import aiohttp

def load_sensors(pg_dsn):
    out = subprocess.run(
        ["psql", pg_dsn, "-tA", "-F", ",", "-c",
         "SELECT sensor_id, kind, zone_id, warn_threshold, crit_threshold "
         "FROM sensor WHERE status='active'"],
        capture_output=True, text=True)
    sensors = []
    for row in csv.reader(out.stdout.strip().splitlines()):
        sid, kind, zone, warn, crit = row[0], row[1], row[2], float(row[3]), float(row[4])
        low_alarm = crit < warn   # o2 / airflow: danger is BELOW the line
        mu = warn * (1.10 if low_alarm else 0.40)
        sensors.append(dict(id=sid, kind=kind, zone=zone, warn=warn, crit=crit,
                            low=low_alarm, mu=mu, base_mu=mu, x=mu))
    if not sensors:
        sys.exit("no sensors loaded — check PG DSN")
    return sensors

def ou_step(s, dt):
    theta, sigma = 0.8, abs(s["base_mu"]) * 0.06
    s["x"] += theta * (s["mu"] - s["x"]) * dt + sigma * math.sqrt(dt) * random.gauss(0, 1)
    return s["x"]

class Stats:
    def __init__(self):
        self.sent = self.accepted = self.warn = self.crit = 0
        self.rate_limited = self.errors = 0
        self.degraded_batches = 0
        self.latencies = []
    def note(self, n_sent, summary, latency):
        self.sent += n_sent
        self.latencies.append(latency)
        if summary is None:
            self.errors += 1; return
        self.accepted += summary["accepted"]
        self.warn += summary["violations_warn"]
        self.crit += summary["violations_critical"]
        self.rate_limited += summary.get("rate_limited", 0)
        self.degraded_batches += summary.get("degraded", False)
    def report(self, elapsed):
        lat = sorted(self.latencies) or [0]
        p = lambda q: lat[min(len(lat)-1, int(q*len(lat)))] * 1000
        return (f"t={elapsed:5.1f}s sent={self.sent} ({self.sent/max(elapsed,1e-9):,.0f}/s) "
                f"accepted={self.accepted} warn={self.warn} crit={self.crit} "
                f"rate_limited={self.rate_limited} degraded_batches={self.degraded_batches} "
                f"req_errors={self.errors} | client ms p50={p(0.5):.1f} p99={p(0.99):.1f}")

async def worker(wid, shard, args, session, stats, t0, flood_sensor):
    tick = args.batch_ms / 1000.0
    per_tick = int(args.rate * tick / args.workers)
    i = 0
    while time.monotonic() - t0 < args.duration:
        start = time.monotonic()
        readings = []
        for _ in range(per_tick):
            s = shard[i % len(shard)]; i += 1
            readings.append({"sensor_id": s["id"],
                             "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                             "value": round(ou_step(s, tick), 4)})
        # the flooder: worker 0 bolts ~100 msg/s from ONE 1Hz sensor onto
        # its batches — the limiter should eat nearly all of it
        if flood_sensor and wid == 0:
            v = ou_step(flood_sensor, tick)
            extra = int(100 * tick)
            readings += [{"sensor_id": flood_sensor["id"],
                          "ts": readings[-1]["ts"], "value": round(v, 4)}] * extra
        try:
            req_start = time.monotonic()
            async with session.post(args.url + "/ingest",
                                    json={"readings": readings},
                                    timeout=aiohttp.ClientTimeout(total=10)) as resp:
                summary = await resp.json() if resp.status == 200 else None
            stats.note(len(readings), summary, time.monotonic() - req_start)
        except Exception:
            stats.note(len(readings), None, time.monotonic() - req_start)
        # hold the tick cadence regardless of request time
        await asyncio.sleep(max(0.0, tick - (time.monotonic() - start)))

async def plume(sensors, args, t0):
    """At 40% of the run: gas leak in the zone with the most CH4 sensors.
    Ramp those sensors' MU from baseline to 1.5x crit over 10 seconds —
    physics does the rest. Alarm counts become scenario-caused and thus
    auditable."""
    await asyncio.sleep(args.duration * 0.4)
    zones = {}
    for s in sensors:
        if s["kind"] == "gas_ch4":
            zones.setdefault(s["zone"], []).append(s)
    zone, victims = max(zones.items(), key=lambda kv: len(kv[1]))
    print(f"\n*** PLUME: zone {zone[:8]}… — {len(victims)} CH4 sensors ramping to 1.5x crit\n")
    steps = 20
    for step in range(1, steps + 1):
        for s in victims:
            s["mu"] = s["base_mu"] + (1.5 * s["crit"] - s["base_mu"]) * step / steps
        await asyncio.sleep(0.5)

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8020")
    ap.add_argument("--pg", required=True, help="postgres DSN for the sensor list")
    ap.add_argument("--rate", type=int, default=2000, help="readings/sec aggregate")
    ap.add_argument("--duration", type=int, default=30)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch-ms", type=int, default=100)
    ap.add_argument("--scenario", default="", help="comma list: plume,flood")
    args = ap.parse_args()

    sensors = load_sensors(args.pg)
    scen = set(filter(None, args.scenario.split(",")))
    flood_sensor = None
    if "flood" in scen:
        flood_sensor = next(s for s in sensors if s["kind"] == "temp")
        print(f"flooder armed: 1Hz sensor {flood_sensor['id'][:13]}… at ~100 msg/s")

    random.shuffle(sensors)
    shards = [sensors[w::args.workers] for w in range(args.workers)]
    stats, t0 = Stats(), time.monotonic()

    conn = aiohttp.TCPConnector(limit=args.workers * 2)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [asyncio.create_task(worker(w, shards[w], args, session, stats, t0, flood_sensor))
                 for w in range(args.workers)]
        if "plume" in scen:
            tasks.append(asyncio.create_task(plume(sensors, args, t0)))
        async def ticker():
            while time.monotonic() - t0 < args.duration:
                await asyncio.sleep(5)
                print(stats.report(time.monotonic() - t0))
        tasks.append(asyncio.create_task(ticker()))
        await asyncio.gather(*tasks, return_exceptions=True)

    print("\n=== FINAL ===")
    print(stats.report(time.monotonic() - t0))
    print(f"violations observed by gateway: warn={stats.warn} crit={stats.crit} "
          f"-> now audit the stream: XLEN delta + spill must equal {stats.warn + stats.crit}")

if __name__ == "__main__":
    asyncio.run(main())
