"""Controlled benchmark; never opens the production database or the network."""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import inspect
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import ROOT, load

FIXTURE_PATH = Path(__file__).with_name("fixtures.json")
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def payload(title, year=2020):
    number = int(hashlib.sha256(title.encode()).hexdigest()[:10], 16)
    return dict(FIXTURE["payload"], Title=title, Year=str(year), imdbID=f"tt{number}")


class Fetcher:
    def __init__(self, delay_ms, mode, years):
        self.delay = delay_ms / 1000
        self.mode, self.years = mode, years
        self.latencies = []
        self.calls = Counter()
        self.lock = threading.Lock()

    def __call__(self, title, key, year):
        start = time.perf_counter()
        try:
            time.sleep(self.delay)
            if self.mode in FIXTURE["fatal_errors"]:
                return {"Response": "False", "Error": FIXTURE["fatal_errors"][self.mode]}
            kind = "matched"
            if self.mode == "errors":
                kind = FIXTURE["error_cycle"][int(title.rsplit(" ", 1)[1]) % len(FIXTURE["error_cycle"])]
            if kind == "timeout":
                # The public HTTP fetcher translates socket timeouts to ConnectionError.
                raise ConnectionError("controlled timeout")
            if kind == "invalid_payload":
                return []
            if kind == "not_found":
                return {"Response": "False", "Error": "Movie not found!"}
            if kind == "different_title":
                return payload("Different film")
            return payload(title, (year or 1994) if kind == "fallback" else self.years[title])
        finally:
            with self.lock:
                self.latencies.append(time.perf_counter() - start)
                self.calls["fallback" if year else "title"] += 1


def statuses(db):
    with duckdb.connect(str(db)) as con:
        return {
            "period_statuses": dict(con.execute(
                "SELECT match_status, count(*) FROM dim_movie GROUP BY 1").fetchall()),
            "request_statuses": dict(con.execute(
                "SELECT request_status, count(*) FROM omdb_request_log GROUP BY 1").fetchall()),
            "facts": con.execute(
                "SELECT count(*), sum(revenue), count(*) FILTER (WHERE theaters IS NULL) "
                "FROM fact_daily_revenue").fetchone(),
        }


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * fraction))] if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", help="Benchmark enrichment from a Git revision (e.g. ad01f37)")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--latencies-ms", type=int, nargs="+", default=[50, 200, 500])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--sample-size", type=int, default=24, help="Number of independent titles")
    parser.add_argument("--seed", type=int, default=20261019)
    parser.add_argument("--scenarios", nargs="+", default=[
        "first_import", "reimport", "pipeline", "cold", "partial", "offline",
        "errors", "invalid_key", "quota_exhausted"])
    args = parser.parse_args()
    known = {"first_import", "reimport", "pipeline", "cold", "partial", "offline",
             "errors", "invalid_key", "quota_exhausted"}
    if (args.repeats < 5 or not 1 <= args.sample_size <= 450
            or any(w < 1 for w in args.workers) or any(ms < 0 for ms in args.latencies_ms)
            or not set(args.scenarios) <= known):
        parser.error("Require >=5 repeats, 1..450 titles, positive workers, nonnegative latency and known scenarios")
    runner = enrich
    with tempfile.TemporaryDirectory(prefix="movies-benchmark-") as directory:
        temp = Path(directory)
        if args.revision:
            source = subprocess.check_output(
                ["git", "show", f"{args.revision}:pipeline/enrich_omdb.py"], cwd=ROOT)
            module_path = temp / "baseline.py"
            module_path.write_bytes(source)
            spec = importlib.util.spec_from_file_location("benchmark_baseline", module_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            runner = module.enrich
        else:
            source = (ROOT / "pipeline/enrich_omdb.py").read_bytes()
        supports_workers = "workers" in inspect.signature(runner).parameters
        if not supports_workers and args.workers != [1]:
            parser.error("This revision is sequential; use --workers 1")
        sample_csv = temp / "sample.csv"
        with sample_csv.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            for i in range(args.sample_size):
                for period in range(2):
                    writer.writerow([f"{i}-{period}", f"{2020 + period}-01-01",
                                     f"Fixture {i:03}", 10000 - i * 2 - period,
                                     "" if i == 0 else 10, "Fixture Studio"])
        empty = temp / "empty.duckdb"
        load(sample_csv, empty)
        years = {f"Fixture {i:03}": FIXTURE["base_year"] for i in range(args.sample_size)}
        snapshots = {"cold": empty}
        for mode in ("partial", "offline"):
            target = temp / f"{mode}.duckdb"
            shutil.copyfile(empty, target)
            with duckdb.connect(str(target)) as con:
                for i, title in enumerate(years):
                    if mode == "offline" or i % FIXTURE["partial_cache_stride"] == 0:
                        con.execute("INSERT INTO omdb_query_cache VALUES (?,0,?,current_timestamp)",
                                    [title, json.dumps(payload(title))])
            snapshots[mode] = target
        full = temp / "full.duckdb"
        load(ROOT / "revenues_per_day.csv", full)
        with duckdb.connect(str(full)) as con:
            full_years = dict(con.execute(
                "SELECT source_title, min(year(run_start_date)) FROM dim_movie GROUP BY 1").fetchall())
            duckdb_threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
        # Reimport uses an identical snapshot containing saved metadata and history.
        runner(full, "fixture", limit=8, fetcher=Fetcher(0, "matched", full_years))
        configurations = []
        for scenario in args.scenarios:
            worker_values = [1] if scenario in ("first_import", "reimport") else args.workers
            delays = args.latencies_ms if scenario == "cold" else [0 if scenario in (
                "first_import", "reimport", "offline") else args.latencies_ms[0]]
            configurations.extend((scenario, workers, delay) for workers in worker_values for delay in delays)
        results = []
        rng = random.Random(args.seed)
        for repetition in range(-1, args.repeats):
            order = list(configurations)
            rng.shuffle(order)
            for scenario, workers, delay in order:
                db = temp / "iteration.duckdb"
                db.unlink(missing_ok=True)
                if scenario not in ("first_import", "pipeline"):
                    shutil.copyfile(full if scenario == "reimport" else snapshots.get(scenario, empty), db)
                fetcher = Fetcher(delay, scenario, full_years if scenario == "pipeline" else years)
                kwargs = {"workers": workers} if supports_workers else {}
                start = time.perf_counter()
                load_seconds = enrichment_seconds = 0
                counts = {}
                if scenario in ("first_import", "reimport", "pipeline"):
                    load(ROOT / "revenues_per_day.csv", db)
                    load_seconds = time.perf_counter() - start
                if scenario not in ("first_import", "reimport"):
                    enrich_start = time.perf_counter()
                    counts = runner(db, None if scenario == "offline" else "fixture",
                                    limit=min(900, args.sample_size * 2), daily_cap=900,
                                    fetcher=fetcher, **kwargs)
                    enrichment_seconds = time.perf_counter() - enrich_start
                elapsed = time.perf_counter() - start
                state = statuses(db)  # Inspection and copies are outside timed stages.
                if scenario in ("first_import", "reimport", "pipeline"):
                    assert state["facts"] == (337818, 205110995141, 161), state
                else:
                    assert sum(state["period_statuses"].values()) == args.sample_size * 2
                assert sum(fetcher.calls.values()) <= args.sample_size * 2
                if scenario == "offline":
                    assert not fetcher.calls
                if repetition >= 0:
                    results.append(dict(scenario=scenario, workers=workers, latency_ms=delay,
                        repetition=repetition, seconds=elapsed, load_seconds=load_seconds,
                        enrichment_seconds=enrichment_seconds, counts=counts,
                        actual_calls=sum(fetcher.calls.values()), fallbacks=fetcher.calls["fallback"],
                        fetch_samples=len(fetcher.latencies),
                        fetch_p50_seconds=percentile(fetcher.latencies, .5),
                        fetch_p95_seconds=percentile(fetcher.latencies, .95),
                        periods_per_second=sum(state["period_statuses"].values()) / elapsed
                            if scenario != "pipeline" else None,
                        requests_per_second=sum(fetcher.calls.values()) / elapsed, **state))
            print(f"{'warmup' if repetition == -1 else f'repeat {repetition + 1}'} complete", flush=True)
        summaries = []
        for scenario, workers, delay in configurations:
            values = [r["seconds"] for r in results if (r["scenario"], r["workers"], r["latency_ms"])
                      == (scenario, workers, delay)]
            summaries.append(dict(scenario=scenario, workers=workers, latency_ms=delay,
                median_seconds=statistics.median(values), min_seconds=min(values),
                max_seconds=max(values), stdev_seconds=statistics.stdev(values)))
        report = dict(metadata=dict(commit=git("rev-parse", args.revision or "HEAD"),
            dirty=bool(git("status", "--porcelain")), implementation_sha256=hashlib.sha256(source).hexdigest(),
            python=platform.python_version(), duckdb=duckdb.__version__, platform=platform.platform(),
            cpu_count=os.cpu_count(), duckdb_threads=duckdb_threads,
            csv_sha256=digest(ROOT / "revenues_per_day.csv"), fixture_sha256=digest(FIXTURE_PATH),
            sample_csv_sha256=digest(sample_csv), seed=args.seed, sample_size=args.sample_size,
            repeats=args.repeats, warmups=1, limit=min(900, args.sample_size * 2), daily_cap=900,
            workers=args.workers, latencies_ms=args.latencies_ms,
            cache_note="OMDb cache only; filesystem cache is uncontrolled"), summary=summaries, runs=results)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        flat = [{k: json.dumps(v) if isinstance(v, (dict, list, tuple)) else v for k, v in row.items()}
                for row in results]
        with args.output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(flat[0]))
            writer.writeheader()
            writer.writerows(flat)
        print(args.output)


if __name__ == "__main__":
    main()
